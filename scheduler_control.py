"""Shared scheduler control file for communication between GUI and headless service."""
import json
import os

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONTROL_FILE = os.path.join(_BASE_DIR, 'scheduler_control.json')
PID_FILE = os.path.join(_BASE_DIR, 'scheduler.pid')
LOG_FILE = os.path.join(_BASE_DIR, 'scheduler.log')


def read_control():
    """Read scheduler control file. Returns dict or None."""
    try:
        with open(CONTROL_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def write_control(running, delay=15, days=1):
    """Write scheduler control file."""
    with open(CONTROL_FILE, 'w') as f:
        json.dump({'running': running, 'delay': delay, 'days': days}, f)


def is_service_running():
    """Check if the scheduler service process is alive."""
    try:
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())
    except (FileNotFoundError, ValueError, OSError):
        return False

    if os.name == 'nt':
        import ctypes
        PROCESS_QUERY_INFORMATION = 0x0400
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_INFORMATION, False, pid)
        if not handle:
            return False
        # Check if process has exited
        exit_code = ctypes.c_ulong(0)
        ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
        ctypes.windll.kernel32.CloseHandle(handle)
        STILL_ACTIVE = 259
        return exit_code.value == STILL_ACTIVE
    else:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
