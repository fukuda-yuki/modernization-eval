"""OS-backed writer exclusion and process identities (never PID-only termination)."""
from contextlib import contextmanager
from datetime import datetime, timezone
import os
from pathlib import Path


@contextmanager
def writer(record):
    path=Path(record)/'writer.lock'
    with path.open('a+b') as stream:
        if path.stat().st_size==0:stream.write(b'0');stream.flush()
        stream.seek(0)
        try:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError as error:raise RuntimeError('Campaign writer is active or unverified') from error
        try:yield
        finally:
            stream.seek(0)
            if os.name=='nt':msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
            else:fcntl.flock(stream,fcntl.LOCK_UN)


def handle_times(handle):
    import ctypes
    from ctypes import wintypes
    values=[wintypes.FILETIME() for _ in range(4)]
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    if not kernel.GetProcessTimes(ctypes.c_void_p(int(handle)),*[ctypes.byref(x) for x in values]):
        raise ctypes.WinError(ctypes.get_last_error())
    ticks=[(v.dwHighDateTime<<32)|v.dwLowDateTime for v in values]
    return {'created_ticks':ticks[0],'exited_at':datetime.fromtimestamp((ticks[1]-116444736000000000)/10000000,timezone.utc).isoformat() if ticks[1] else None}


def identity(pid):
    if type(pid) is not int or pid<=0:raise ValueError('Invalid process identity')
    if os.name=='nt':
        import ctypes
        kernel=ctypes.WinDLL('kernel32',use_last_error=True)
        kernel.OpenProcess.restype=ctypes.c_void_p
        h=kernel.OpenProcess(0x1000,False,pid)
        if not h:
            if ctypes.get_last_error() in (87,1168):return None
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            times=handle_times(h)
            if times['exited_at']:return None
            return {'pid':pid,'created':str(times['created_ticks'])}
        finally:kernel.CloseHandle(ctypes.c_void_p(h))
    try:
        fields=Path('/proc',str(pid),'stat').read_text().split(') ',1)[1].split()
        if fields[0]=='Z':return None
        return {'pid':pid,'created':fields[19]}
    except FileNotFoundError:return None


def alive(expected):
    actual=identity(expected['pid'])
    if actual is None:return False
    if actual!=expected:raise ValueError('PID creation identity mismatch; no process action permitted')
    return True


def terminate(expected):
    """Explicit stop of the exact registered process through a retained OS handle."""
    if os.name=='nt':
        import ctypes
        kernel=ctypes.WinDLL('kernel32',use_last_error=True);kernel.OpenProcess.restype=ctypes.c_void_p
        h=kernel.OpenProcess(0x1000|0x1|0x100000,False,expected['pid'])
        if not h:
            if ctypes.get_last_error() in (87,1168):return False
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            observed=handle_times(h)
            if str(observed['created_ticks'])!=expected['created']:raise ValueError('PID creation identity mismatch')
            if observed['exited_at']:return False
            if not kernel.TerminateProcess(ctypes.c_void_p(h),143):raise ctypes.WinError(ctypes.get_last_error())
            if kernel.WaitForSingleObject(ctypes.c_void_p(h),15000)!=0:raise TimeoutError('Owned process stop unconfirmed')
            return True
        finally:kernel.CloseHandle(ctypes.c_void_p(h))
    import signal
    fd=os.pidfd_open(expected['pid'])
    try:
        if not alive(expected):return False
        signal.pidfd_send_signal(fd,signal.SIGTERM)
        return True
    finally:os.close(fd)
