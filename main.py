import argparse
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta

from config import setup_logging
from manager import FTPSiteManager
from scheduler_control import CONTROL_FILE, PID_FILE, LOG_FILE, read_control, write_control, is_service_running

logger = logging.getLogger(__name__)


def run_headless(manager):
    """Run the scheduler as a service. Reads settings from control file."""
    # Write PID file
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))

    def stop(sig, frame):
        logger.info("Received signal %s, stopping scheduler service...", sig)
        write_control(False)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    logger.info("Scheduler service started (PID %d)", os.getpid())

    try:
        while True:
            ctrl = read_control()
            if not ctrl or not ctrl.get('running', False):
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
    finally:
        # Cleanup PID file
        try:
            os.remove(PID_FILE)
        except OSError:
            pass
        logger.info("Scheduler service stopped.")


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
        run_headless(manager)
    else:
        from gui import FTPSiteGUI
        app = FTPSiteGUI(manager)
        app.run()
