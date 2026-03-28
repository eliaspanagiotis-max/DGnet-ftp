import base64
import json
import logging
import os
import smtplib
import socket
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

SMTP_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'smtp_config.json')
NOTIF_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'notifications_config.json')


def load_smtp():
    if not os.path.exists(SMTP_CONFIG_FILE):
        return None
    try:
        with open(SMTP_CONFIG_FILE) as f:
            cfg = json.load(f)
        if cfg.get('password'):
            cfg['password'] = base64.b64decode(cfg['password']).decode()
        return cfg
    except Exception as e:
        logger.error("Failed to load SMTP config: %s", e)
        return None


def save_smtp(host, port, user, password, from_addr, use_tls=True):
    cfg = {
        'host': host,
        'port': int(port),
        'user': user,
        'password': base64.b64encode(password.encode()).decode(),
        'from': from_addr,
        'tls': use_tls,
    }
    with open(SMTP_CONFIG_FILE, 'w') as f:
        json.dump(cfg, f, indent=2)


_RECIPIENT_DEFAULTS = {'on_start': True, 'on_stop': True, 'on_inactive': True,
                       'on_download': False, 'on_last_file': False,
                       'on_mount_alert': True, 'on_ping_alert': True,
                       'repeat_minutes': 30}


def load_notifications():
    if not os.path.exists(NOTIF_CONFIG_FILE):
        return {'enabled': False, 'recipients': []}
    try:
        with open(NOTIF_CONFIG_FILE) as f:
            cfg = json.load(f)
        # Migrate old flat email list format
        if 'emails' in cfg and 'recipients' not in cfg:
            cfg['recipients'] = [
                dict(_RECIPIENT_DEFAULTS, email=e) for e in cfg.pop('emails')
            ]
        for r in cfg.get('recipients', []):
            for k, v in _RECIPIENT_DEFAULTS.items():
                r.setdefault(k, v)
        return cfg
    except Exception:
        return {'enabled': False, 'recipients': []}


def get_inactive_repeat_recipients():
    """Return list of {email, repeat_minutes} for recipients subscribed to on_inactive
    with repeat_minutes > 0. Returns empty list if notifications disabled."""
    notif = load_notifications()
    if not notif.get('enabled'):
        return []
    return [
        {'email': r['email'], 'repeat_minutes': r.get('repeat_minutes', 0)}
        for r in notif.get('recipients', [])
        if r.get('on_inactive', True) and r.get('repeat_minutes', 0) > 0 and r.get('email')
    ]


def save_notifications(enabled, recipients):
    """recipients: list of dicts with keys: email, on_start, on_stop, on_inactive, on_download"""
    cfg = {'enabled': bool(enabled), 'recipients': recipients}
    with open(NOTIF_CONFIG_FILE, 'w') as f:
        json.dump(cfg, f, indent=2)


def send_notification(subject, body, event=None, override_recipients=None):
    """Send email to recipients subscribed to this event. Returns (success, error_msg).
    If override_recipients is given (list of email strings), send only to those addresses."""
    notif = load_notifications()
    if not notif.get('enabled'):
        return True, None
    if override_recipients is not None:
        recipients = [e for e in override_recipients if e]
    else:
        all_recipients = notif.get('recipients', [])
        if event:
            recipients = [r['email'] for r in all_recipients if r.get(event, True)]
        else:
            recipients = [r['email'] for r in all_recipients]
    if not recipients:
        return True, None

    smtp = load_smtp()
    if not smtp:
        return False, "SMTP not configured"

    msg = MIMEText(body, 'plain')
    msg['Subject'] = f"[DGnet FTP Monitor] {subject}"
    msg['From'] = smtp.get('from', smtp['user'])
    msg['To'] = ', '.join(recipients)

    try:
        port = smtp['port']
        use_ssl = (port == 465) or smtp.get('ssl', False)
        ctx = None
        if use_ssl or smtp.get('tls', True):
            import ssl as _ssl
            ctx = _ssl.create_default_context()
        if use_ssl:
            conn = smtplib.SMTP_SSL(smtp['host'], port, timeout=15, context=ctx)
        else:
            conn = smtplib.SMTP(smtp['host'], port, timeout=15)
        with conn as s:
            if not use_ssl and smtp.get('tls', True):
                s.starttls(context=ctx)
            if smtp.get('user') and smtp.get('password'):
                s.login(smtp['user'], smtp['password'])
            s.sendmail(msg['From'], recipients, msg.as_string())
        logger.info("Notification sent: %s → %s", subject, recipients)
        return True, None
    except Exception as e:
        logger.error("Failed to send notification: %s", e)
        return False, str(e)


def notify_scheduler_started():
    host = socket.gethostname()
    send_notification(
        "Scheduler Started",
        f"The DGnet FTP scheduler has started on host '{host}'.\n\nIt will scan at the configured interval.",
        event='on_start'
    )


def notify_scheduler_stopped(reason="unknown"):
    host = socket.gethostname()
    send_notification(
        "Scheduler Stopped",
        f"The DGnet FTP scheduler has stopped on host '{host}'.\n\nReason: {reason}",
        event='on_stop'
    )


def notify_scheduler_inactive(reason="unknown"):
    host = socket.gethostname()
    send_notification(
        "WARNING: Scheduler Not Active",
        f"The DGnet FTP scheduler is NO LONGER ACTIVE on host '{host}'.\n\nReason: {reason}\n\nPlease restart it from the GUI.",
        event='on_inactive'
    )


def notify_scheduler_crashed(traceback_str):
    host = socket.gethostname()
    send_notification(
        "WARNING: Scheduler CRASHED",
        f"The DGnet FTP scheduler has crashed on host '{host}'.\n\n{traceback_str}",
        event='on_inactive'
    )


def notify_mount_alert(name, path, mounted):
    host = socket.gethostname()
    if mounted:
        send_notification(
            f"Mountpoint Restored: {name}",
            f"Mountpoint '{name}' ({path}) is now mounted on host '{host}'.",
            event='on_mount_alert'
        )
    else:
        send_notification(
            f"WARNING: Mountpoint Not Mounted: {name}",
            f"Mountpoint '{name}' ({path}) is NOT mounted on host '{host}'.\n\nPlease check the system.",
            event='on_mount_alert'
        )


def notify_ping_alert(name, host, reachable):
    h = socket.gethostname()
    if reachable:
        send_notification(
            f"Host Restored: {name}",
            f"Host '{name}' ({host}) is reachable again on '{h}'.",
            event='on_ping_alert'
        )
    else:
        send_notification(
            f"WARNING: Host Unreachable: {name}",
            f"Host '{name}' ({host}) is NOT reachable from '{h}'.\n\nPlease check the network.",
            event='on_ping_alert'
        )


def notify_last_file_status(site_results):
    """Send a per-station summary of the most recent expected file after each scan cycle."""
    if not site_results:
        return
    host = socket.gethostname()
    missing = [r for r in site_results if r['status'] != 'ok']
    subject = (f"Last File Check — {len(missing)} station(s) missing"
               if missing else f"Last File Check — All {len(site_results)} station(s) OK")
    lines = [f"Last-file status after scan on host '{host}':", ""]
    for r in site_results:
        mark = "OK" if r['status'] == 'ok' else "!!"
        lines.append(f"  [{mark}]  {r['site']:20s}  {r['file']}  ({r['date']})  — {r['status']}")
    send_notification(subject, "\n".join(lines), event='on_last_file')


def notify_files_downloaded(files):
    host = socket.gethostname()
    file_list = '\n'.join(f"  - {f}" for f in files)
    send_notification(
        f"Files Downloaded ({len(files)})",
        f"The scheduler downloaded {len(files)} file(s) on host '{host}':\n\n{file_list}",
        event='on_download'
    )
