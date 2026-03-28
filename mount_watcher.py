"""
Mount watcher service for DGnet.
Monitors configured mountpoints and pings stations, sends notifications on state changes.
"""
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BASE_DIR)

from notifier import notify_mount_alert, notify_ping_alert

CONFIG_FILE       = os.path.join(_BASE_DIR, 'mount_watcher_config.json')
SITES_CONFIG_FILE = os.path.join(_BASE_DIR, 'sites_config.json')
LOG_FILE          = os.path.join(_BASE_DIR, 'scheduler.log')
PID_FILE          = os.path.join(_BASE_DIR, 'mount_watcher.pid')
DEFAULT_INTERVAL  = 1  # minutes

log = logging.getLogger('mount_watcher')
_running = True


def load_config():
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {'mountpoints': [], 'interval': DEFAULT_INTERVAL,
                'ping_enabled': False, 'ping_interval': DEFAULT_INTERVAL}


def load_ping_stations():
    try:
        with open(SITES_CONFIG_FILE) as f:
            sites = json.load(f)
        return [s for s in sites if s.get('ping_check', False)]
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return []


def is_reachable(host):
    try:
        result = subprocess.run(
            ['ping', '-c', '1', '-W', '2', host],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def check_mounts(cfg, prev_state):
    mountpoints = cfg.get('mountpoints', [])
    configured_paths = set()
    for mp in mountpoints:
        path = mp['path'] if isinstance(mp, dict) else mp
        name = mp.get('name', path) if isinstance(mp, dict) else path
        configured_paths.add(path)
        try:
            mounted = os.path.ismount(path)
        except OSError:
            mounted = False
        prev = prev_state.get(path)
        if prev is None:
            prev_state[path] = mounted
            if not mounted:
                log.warning("Mountpoint NOT mounted on startup: %s (%s)", name, path)
                notify_mount_alert(name, path, mounted=False)
            else:
                log.info("Mountpoint OK: %s (%s)", name, path)
        elif prev and not mounted:
            log.warning("Mountpoint LOST: %s (%s)", name, path)
            notify_mount_alert(name, path, mounted=False)
            prev_state[path] = False
        elif not prev and mounted:
            log.info("Mountpoint RESTORED: %s (%s)", name, path)
            notify_mount_alert(name, path, mounted=True)
            prev_state[path] = True
    for p in list(prev_state):
        if p not in configured_paths:
            del prev_state[p]


def check_pings(prev_ping):
    stations = load_ping_stations()
    configured = set()
    for s in stations:
        host = s.get('host', '')
        name = s.get('station_code') or s.get('name', host)
        if not host:
            continue
        configured.add(host)
        reachable = is_reachable(host)
        prev = prev_ping.get(host)
        if prev is None:
            prev_ping[host] = reachable
            if not reachable:
                log.warning("Host unreachable on startup: %s (%s)", name, host)
                notify_ping_alert(name, host, reachable=False)
            else:
                log.info("Host reachable: %s (%s)", name, host)
        elif prev and not reachable:
            log.warning("Host LOST: %s (%s)", name, host)
            notify_ping_alert(name, host, reachable=False)
            prev_ping[host] = False
        elif not prev and reachable:
            log.info("Host RESTORED: %s (%s)", name, host)
            notify_ping_alert(name, host, reachable=True)
            prev_ping[host] = True
    for h in list(prev_ping):
        if h not in configured:
            del prev_ping[h]


def run():
    global _running

    def stop(sig, frame):
        global _running
        _running = False
        log.info("Received signal %s, stopping.", sig)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    if hasattr(signal, 'SIGHUP'):
        signal.signal(signal.SIGHUP, signal.SIG_IGN)

    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))

    log.info("Mount watcher started (PID %d)", os.getpid())

    prev_state = {}  # path -> bool
    prev_ping  = {}  # host -> bool

    last_mount_check = 0.0
    last_ping_check  = 0.0

    try:
        while _running:
            now = time.time()
            cfg = load_config()
            interval     = max(1, int(cfg.get('interval', DEFAULT_INTERVAL))) * 60
            ping_enabled = cfg.get('ping_enabled', False)

            if now - last_mount_check >= interval:
                check_mounts(cfg, prev_state)
                last_mount_check = now

            if ping_enabled and now - last_ping_check >= interval:
                check_pings(prev_ping)
                last_ping_check = now

            time.sleep(1)
    finally:
        log.info("Mount watcher stopped.")
        try:
            os.remove(PID_FILE)
        except OSError:
            pass


if __name__ == '__main__':
    log_handler = logging.FileHandler(LOG_FILE)
    log_handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'))
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(log_handler)
    run()
