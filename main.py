import argparse
import atexit
import logging
import os
import signal
import sys
import time
import traceback
from datetime import datetime, timedelta

from config import setup_logging
from manager import FTPSiteManager
from scheduler_control import CONTROL_FILE, PID_FILE, LOG_FILE, read_control, write_control, is_service_running

logger = logging.getLogger(__name__)

_stop_reason = "unknown (process killed or crashed)"


def _atexit_log():
    logger.info("Process exiting. Stop reason: %s", _stop_reason)


def run_headless(manager):
    """Run the scheduler as a service. Reads settings from control file."""
    global _stop_reason

    # Write PID file
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))

    def stop(sig, frame):
        global _stop_reason
        _stop_reason = f"signal {sig}"
        logger.info("Received signal %s, stopping scheduler service...", sig)
        write_control(False)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    if hasattr(signal, 'SIGBREAK'):  # Windows Ctrl+Break / task-manager kill
        signal.signal(signal.SIGBREAK, stop)

    logger.info("Scheduler service started (PID %d)", os.getpid())

    try:
        while True:
            ctrl = read_control()
            if not ctrl or not ctrl.get('running', False):
                _stop_reason = "control file set running=false"
                logger.info("Control file says stop. Exiting.")
                break

            delay_minutes = ctrl.get('delay', 15)
            days_back = ctrl.get('days', 1)

            # Calculate next run time
            now = datetime.now()
            candidate = now.replace(minute=delay_minutes, second=0, microsecond=0)
            if candidate <= now:
                candidate += timedelta(hours=1)
            next_run = candidate
            logger.info("Next scan at %s (delay=%d, days=%d)", next_run.strftime("%H:%M:%S"), delay_minutes, days_back)

            # Wait, checking control file every 5 seconds
            while datetime.now() < next_run:
                ctrl = read_control()
                if not ctrl or not ctrl.get('running', False):
                    _stop_reason = "control file set running=false (during wait)"
                    logger.info("Control file says stop. Exiting.")
                    return
                # Re-read delay in case GUI changed it
                new_delay = ctrl.get('delay', 15)
                if new_delay != delay_minutes:
                    delay_minutes = new_delay
                    candidate = datetime.now().replace(minute=delay_minutes, second=0, microsecond=0)
                    if candidate <= datetime.now():
                        candidate += timedelta(hours=1)
                    next_run = candidate
                    logger.info("Delay changed, next scan at %s", next_run.strftime("%H:%M:%S"))
                time.sleep(5)

            # Scan
            logger.info("Starting scan...")
            try:
                log = manager.scan_all(
                    days_back,
                    progress_cb=lambda msg: logger.info(msg),
                    file_cb=lambda site, f: logger.debug("[%s] %s", site, f),
                )
                manager.auto_download_completed(log, delay_minutes)

                total = sum(len(items) for items in log.log.values())
                missing = sum(
                    1 for items in log.log.values()
                    for item in items
                    if item['status'] in ('missing locally', 'size mismatch')
                )
                logger.info("Scan complete: %d files checked, %d missing/mismatched", total, missing)
            except Exception:
                logger.error("Unhandled exception during scan:\n%s", traceback.format_exc())
                raise

    except Exception:
        _stop_reason = "unhandled exception (see ERROR above)"
        logger.critical("Scheduler crashed with unhandled exception:\n%s", traceback.format_exc())
        raise
    finally:
        # Cleanup PID file
        try:
            os.remove(PID_FILE)
        except OSError:
            pass
        logger.info("Scheduler service stopped. Reason: %s", _stop_reason)


if __name__ == "__main__":
    setup_logging()

    parser = argparse.ArgumentParser(description="DGnet FTP Monitor")
    parser.add_argument("--headless", action="store_true",
                        help="Run scheduler as background service (no GUI)")
    args = parser.parse_args()

    manager = FTPSiteManager()

    if args.headless:
        # Add file logging for service mode
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                                                     datefmt='%Y-%m-%d %H:%M:%S'))
        logging.getLogger().addHandler(file_handler)
        atexit.register(_atexit_log)
        run_headless(manager)
    else:
        from gui import FTPSiteGUI
        app = FTPSiteGUI(manager)
        app.run()
