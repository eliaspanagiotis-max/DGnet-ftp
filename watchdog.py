"""
Watchdog process for the DGnet FTP scheduler.
Monitors the scheduler PID and sends a notification if it dies unexpectedly.
Started alongside the scheduler by the GUI.
"""
import os
import sys
import time
import logging

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BASE_DIR)

from scheduler_control import PID_FILE, read_control, is_service_running
from notifier import notify_scheduler_inactive, get_inactive_repeat_recipients, send_notification

WATCHDOG_PID_FILE = os.path.join(_BASE_DIR, 'watchdog.pid')
LOG_FILE = os.path.join(_BASE_DIR, 'scheduler.log')
POLL_INTERVAL = 10  # seconds


def _repeat_alert_loop(scheduler_pid, log):
    """After initial 'not active' alert, resend reminder emails per recipient's repeat_minutes."""
    import socket
    host = socket.gethostname()
    subject = "WARNING: Scheduler Still Not Active"
    body_tpl = (
        "The DGnet FTP scheduler is STILL NOT ACTIVE on host '{host}'.\n\n"
        "It has not been restarted since process {pid} was killed.\n\n"
        "Please restart it from the GUI."
    )

    repeat_cfg = get_inactive_repeat_recipients()
    if not repeat_cfg:
        log.info("No repeat notifications configured — watchdog exiting.")
        return

    now = time.time()
    last_sent = {r['email']: now for r in repeat_cfg}
    intervals = {r['email']: r['repeat_minutes'] * 60 for r in repeat_cfg}

    log.info("Repeat alert loop started for %d recipient(s): %s",
             len(repeat_cfg), [r['email'] for r in repeat_cfg])

    while True:
        time.sleep(POLL_INTERVAL)

        if is_service_running():
            log.info("Scheduler has restarted — stopping repeat alerts.")
            return

        ctrl = read_control()
        if ctrl is not None and not ctrl.get('running', True):
            log.info("Control file says stop — stopping repeat alerts.")
            return

        now = time.time()
        repeat_cfg = get_inactive_repeat_recipients()
        intervals = {r['email']: r['repeat_minutes'] * 60 for r in repeat_cfg}

        due = [
            email for email, last in list(last_sent.items())
            if email in intervals and now - last >= intervals[email]
        ]
        for r in repeat_cfg:
            if r['email'] not in last_sent:
                last_sent[r['email']] = now - intervals.get(r['email'], 0)  # send immediately

        if due:
            body = body_tpl.format(host=host, pid=scheduler_pid)
            send_notification(subject, body, override_recipients=due)
            for email in due:
                last_sent[email] = now


def run(scheduler_pid):
    log_handler = logging.FileHandler(LOG_FILE)
    log_handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] watchdog: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'))
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(log_handler)
    log = logging.getLogger('watchdog')

    with open(WATCHDOG_PID_FILE, 'w') as f:
        f.write(str(os.getpid()))

    log.info("Watchdog started, monitoring scheduler PID %d", scheduler_pid)

    try:
        while True:
            time.sleep(POLL_INTERVAL)

            alive = is_service_running()
            ctrl = read_control()
            intentional_stop = ctrl is not None and not ctrl.get('running', True)

            if not alive:
                if intentional_stop:
                    log.info("Scheduler stopped intentionally — watchdog exiting.")
                else:
                    log.warning("Scheduler PID %d is gone unexpectedly!", scheduler_pid)
                    notify_scheduler_inactive(f"Process {scheduler_pid} died (SIGKILL or crash without traceback)")
                    _repeat_alert_loop(scheduler_pid, log)
                break
    finally:
        try:
            os.remove(WATCHDOG_PID_FILE)
        except OSError:
            pass
        log.info("Watchdog exiting.")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: watchdog.py <scheduler_pid>")
        sys.exit(1)
    run(int(sys.argv[1]))
