"""Read-only process identity and explicit job/session separation.

No PID-based termination and no changes to an existing Windows job. Windows
launch must already be outside a Job (the PC-verified Windows Terminal route).
"""
import os
from pathlib import Path
import subprocess


def _kernel():
    import ctypes
    from ctypes import wintypes as w
    k = ctypes.WinDLL('kernel32', use_last_error=True)
    for name, args, result in (
        ('OpenProcess', [w.DWORD, w.BOOL, w.DWORD], w.HANDLE),
        ('GetCurrentProcess', [], w.HANDLE),
        ('GetProcessTimes', [w.HANDLE, ctypes.POINTER(w.FILETIME), ctypes.POINTER(w.FILETIME),
                            ctypes.POINTER(w.FILETIME), ctypes.POINTER(w.FILETIME)], w.BOOL),
        ('WaitForSingleObject', [w.HANDLE, w.DWORD], w.DWORD),
        ('CloseHandle', [w.HANDLE], w.BOOL),
        ('IsProcessInJob', [w.HANDLE, w.HANDLE, ctypes.POINTER(w.BOOL)], w.BOOL)):
        function = getattr(k, name); function.argtypes = args; function.restype = result
    return k


class ProcessIdentity:
    """Retain the Windows process handle, as the historical wave owner does.

    Creation time additionally rejects a PID recycled before the handle opens.
    Linux tests use boot identity + /proc start ticks; no signal is sent.
    """
    def __init__(self, pid, expected=None):
        if type(pid) is not int or pid <= 0:
            raise ValueError('Positive process PID required')
        self.pid, self.handle = pid, None
        if os.name == 'nt':
            import ctypes
            from ctypes import wintypes as w
            self.kernel = _kernel()
            self.handle = self.kernel.OpenProcess(0x100000 | 0x1000, False, pid)
            if not self.handle: raise OSError(ctypes.get_last_error(), 'Owner process unavailable')
            values = [w.FILETIME() for _ in range(4)]
            if not self.kernel.GetProcessTimes(self.handle, *(ctypes.byref(v) for v in values)):
                self.close(); raise OSError('Owner process creation time unavailable')
            self.record = dict(pid=pid, platform='windows',
                started=(values[0].dwHighDateTime << 32) | values[0].dwLowDateTime)
        else:
            self.record = self._linux()[0]
        if expected is not None and self.record != expected:
            self.close(); raise ValueError('Process start identity mismatch')

    def _linux(self):
        stat = Path('/proc', str(self.pid), 'stat').read_text().rsplit(')', 1)[1].split()
        return (dict(pid=self.pid, platform='linux', started=int(stat[19]),
                     boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()), stat[0])

    def alive(self):
        if os.name == 'nt':
            value = self.kernel.WaitForSingleObject(self.handle, 0)
            if value not in (0, 258): raise OSError('Owner process state unavailable')
            return value == 258
        try: record, state = self._linux()
        except FileNotFoundError: return False
        return record == self.record and state not in ('Z', 'X')

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle); self.handle = None

    def __enter__(self): return self
    def __exit__(self, *_): self.close()


def current_identity():
    with ProcessIdentity(os.getpid()) as process: return process.record


def assert_durable_host():
    if os.name == 'nt':
        import ctypes
        from ctypes import wintypes as w
        k = _kernel(); contained = w.BOOL()
        if not k.IsProcessInJob(k.GetCurrentProcess(), None, ctypes.byref(contained)):
            raise OSError('Cannot establish Windows job separation')
        if contained.value: raise RuntimeError('Supervisor is still in a Windows job; acquisition refused')
        return dict(platform='windows', in_job=False)
    if os.getsid(0) != os.getpid(): raise RuntimeError('Supervisor is not its own session leader')
    return dict(platform='linux', session_id=os.getsid(0))


def persistent_popen(command, **kwargs):
    # Normal Windows Terminal launch is checked on the parent and again inside
    # the child. No attempt to escape/reconfigure a tool-owned outer Job.
    if os.name == 'nt':
        assert_durable_host()
        kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW
    else:
        kwargs['start_new_session'] = True
    return subprocess.Popen(command, stdin=subprocess.DEVNULL, **kwargs)
