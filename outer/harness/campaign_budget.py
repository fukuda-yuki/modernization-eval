"""Trusted gateway-only, transactional campaign admission. No credentials.

This optional boundary does not change historical gateways. Reservations survive
crashes; only the owner creates the database and workers never mount its directory.
"""
from contextlib import closing, contextmanager
import json
from pathlib import Path
import re
import sqlite3
import time


class BudgetDenied(RuntimeError):
    pass


class Budget:
    def __init__(self, path):
        self.path = Path(path).resolve()

    @classmethod
    def create(cls, path, *, campaign_id, plan_sha256, max_requests, max_tokens, deadline):
        if (not campaign_id or not re.fullmatch(r'[a-f0-9]{64}', plan_sha256)
                or any(type(n) is not int or n <= 0 for n in (max_requests, max_tokens))
                or not isinstance(deadline, (int, float)) or deadline <= 0):
            raise ValueError('Explicit campaign identity and positive bounds required')
        budget = cls(path)
        budget.path.parent.mkdir(parents=True, exist_ok=True)
        budget.path.touch(exist_ok=False)
        with closing(sqlite3.connect(budget.path)) as db:
            db.executescript('''
                CREATE TABLE policy (singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    campaign_id TEXT NOT NULL, plan_sha256 TEXT NOT NULL,
                    max_requests INTEGER NOT NULL, max_tokens INTEGER NOT NULL,
                    deadline REAL NOT NULL, closed_reason TEXT);
                CREATE TABLE requests (request_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                    reserved_at REAL NOT NULL, usage_json TEXT, terminal_status TEXT,
                    input_tokens INTEGER, output_tokens INTEGER);
            ''')
            db.execute('INSERT INTO policy VALUES (1,?,?,?,?,?,NULL)',
                       (campaign_id, plan_sha256, max_requests, max_tokens, deadline))
            db.commit()
        return budget

    @contextmanager
    def transaction(self):
        if not self.path.is_file():
            raise FileNotFoundError('Campaign admission database missing')
        db = sqlite3.connect(self.path.as_uri()+'?mode=rw', uri=True, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def reserve(self, request_id, run_id, *, now=None):
        if not request_id or not run_id:
            raise ValueError('Request and Run identity required')
        at = time.time() if now is None else now
        with self.transaction() as db:
            policy = db.execute('SELECT * FROM policy').fetchone()
            count, observed = db.execute('SELECT COUNT(*), COALESCE(SUM('
                'COALESCE(input_tokens,0)+COALESCE(output_tokens,0)),0) FROM requests').fetchone()
            reason = (policy['closed_reason'] or ('deadline' if at >= policy['deadline'] else None)
                      or ('request_limit' if count >= policy['max_requests'] else None)
                      or ('observed_token_limit' if observed >= policy['max_tokens'] else None))
            if reason:
                raise BudgetDenied(reason)
            try:
                db.execute('INSERT INTO requests(request_id,run_id,reserved_at) VALUES (?,?,?)',
                           (request_id, run_id, at))
            except sqlite3.IntegrityError as exc:
                raise BudgetDenied('duplicate_request') from exc

    def finish(self, request_id, usage, status):
        if not isinstance(status,str) or not status:
            raise ValueError('Nonempty terminal status required')
        usage = usage if isinstance(usage, dict) else None
        encoded = json.dumps(usage, sort_keys=True, separators=(',', ':'))
        values = [(usage or {}).get(k) for k in ('input_tokens', 'output_tokens')]
        values = [v if type(v) is int and v >= 0 else None for v in values]
        with self.transaction() as db:
            row = db.execute('SELECT * FROM requests WHERE request_id=?', (request_id,)).fetchone()
            if row is None:
                raise ValueError('Unreserved request cannot report usage')
            if row['terminal_status'] is not None:
                if (row['usage_json'], row['terminal_status']) != (encoded, status):
                    raise ValueError('Terminal usage changed')
                return
            db.execute('UPDATE requests SET usage_json=?,terminal_status=?,input_tokens=?,output_tokens=? '
                       'WHERE request_id=?', (encoded, status, *values, request_id))

    def close(self, reason):
        if not reason:
            raise ValueError('Stop reason required')
        with self.transaction() as db:
            db.execute('UPDATE policy SET closed_reason=COALESCE(closed_reason,?)', (reason,))

    def snapshot(self):
        with self.transaction() as db:
            policy = dict(db.execute('SELECT * FROM policy').fetchone())
            rows = db.execute('SELECT * FROM requests').fetchall()
        known = sum((r['input_tokens'] or 0)+(r['output_tokens'] or 0) for r in rows)
        missing = sum(r['input_tokens'] is None or r['output_tokens'] is None for r in rows)
        return {**policy, 'reserved_requests': len(rows), 'observed_tokens_lower_bound': known,
                'observed_tokens': None if missing else known, 'missing_usage_requests': missing,
                'inflight_requests': sum(r['terminal_status'] is None for r in rows)}
