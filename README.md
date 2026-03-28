# DGnet FTP Monitor

Monitors FTP/SFTP stations of the Greek GNSS Network for expected data files and automatically downloads any that are missing.

## Features

- Scans FTP and SFTP sites for expected files based on configurable filename patterns
- Auto-downloads missing or size-mismatched files after a configurable delay
- Detects actively uploading files (`.A` sidecar detection) to avoid partial downloads
- Supports dynamic remote paths and filenames using `strftime` format codes
- Handles hourly and daily file frequencies
- Tkinter GUI for manual scanning, downloading, and scheduler control
- Headless scheduler mode for unattended background operation
- Watchdog process that sends alerts if the scheduler dies unexpectedly
- Email notifications for: scheduler start/stop/crash, files downloaded

## Requirements

```
pip install paramiko
```

Standard library only otherwise (tkinter, ftplib, smtplib).

## Configuration

### Sites — `sites_config.json`

Copy `sites_config.example.json` to `sites_config.json` and fill in your stations. The file is gitignored. Each station entry:

| Field | Description |
|---|---|
| `name` | Station label (displayed in GUI) |
| `host` | FTP/SFTP hostname or IP |
| `protocol` | `"ftp"` or `"sftp"` |
| `port` | Port number (default: 21 for FTP, 22 for SFTP) |
| `user` / `password` | Credentials |
| `path` | Remote directory path. Supports `strftime` codes (e.g. `/%Y%m/%d/`) |
| `pattern` | Filename pattern with `strftime` codes (e.g. `SITE%y%m%d%H00a.T00`) |
| `frequency` | `"hourly"` or `"daily"` |
| `use_letter_hour` | If `true`, replaces `%H` with a letter (`a`–`x`) in the filename pattern |
| `output_dir` | Local download directory. Supports `strftime` codes (e.g. `./downloads/SITE/%Y/%m`) |
| `network` | Network label shown in GUI |
| `rate` | Data rate label (e.g. `"30s"`, `"1s"`, `"10Hz"`) |
| `station_code` | 4-character station code |
| `format` | Receiver format label (e.g. `"Topcon"`, `"Trimble"`) |
| `external_clock` | Boolean, informational only |

### SMTP — `smtp_config.json`

```json
{
  "host": "smtp.example.gr",
  "port": 465,
  "user": "username",
  "password": "base64encodedpassword",
  "from": "sender@label",
  "tls": true
}
```

The password is stored base64-encoded.

### Notifications — `notifications_config.json`

```json
{
  "enabled": true,
  "recipients": [
    {
      "email": "user@example.com",
      "on_start": true,
      "on_stop": true,
      "on_inactive": true,
      "repeat_minutes": 30,
      "on_download": true
    }
  ]
}
```

Each recipient can individually opt in/out of each event type. `repeat_minutes` controls how often the watchdog re-sends the "scheduler not active" alert.

## Running

### GUI mode

```bash
python3 main.py
```

Launches the Tkinter interface. From the GUI you can scan stations, view file status, download missing files manually, and start/stop the background scheduler service.

### Headless scheduler

```bash
python3 main.py --headless
```

Runs as a background service. Reads settings from `scheduler_control.json` (`delay` and `days` fields). Scans all sites once per hour at the configured minute offset, then auto-downloads any files that are missing and older than `delay` minutes.

The GUI and headless process communicate through `scheduler_control.json`. You can run both simultaneously — the GUI detects a running service and connects to it.

## Systemd User Service (Linux)

The scheduler runs as a systemd user service so it survives desktop session logouts and restarts automatically on failure.

**Service file:** `~/.config/systemd/user/dgnet-ftp-scheduler.service`

### Setup (already done on this host)

```bash
# Enable lingering so the service runs without an active login session
loginctl enable-linger $USER

# Reload and enable
systemctl --user daemon-reload
systemctl --user enable dgnet-ftp-scheduler.service
systemctl --user start dgnet-ftp-scheduler.service
```

### Common commands

```bash
# Status
systemctl --user status dgnet-ftp-scheduler.service

# Start / stop / restart
systemctl --user start dgnet-ftp-scheduler.service
systemctl --user stop dgnet-ftp-scheduler.service
systemctl --user restart dgnet-ftp-scheduler.service

# Live logs
journalctl --user -u dgnet-ftp-scheduler.service -f

# Full log file
tail -f scheduler.log
```

The service uses `Restart=on-failure` with a 10-second delay, so it will recover from crashes automatically.

## File Status Reference

| Status | Meaning |
|---|---|
| `ok` | File present locally with correct size |
| `missing locally` | File exists on remote but not locally |
| `missing remotely` | File expected but not found on remote |
| `size mismatch` | File present locally but size differs from remote |
| `uploading` | File not yet complete on remote (`.A` sidecar detected and growing) |
| `new` | Current-hour file that just appeared on remote |
| `scheduled` | File not yet due (future timestamp) |
| `connection failed` | Could not connect to the remote site |
| `ok (offline)` | Connection failed but file exists locally |

## Runtime Files

| File | Purpose |
|---|---|
| `scheduler_control.json` | GUI↔service communication (`running`, `delay`, `days`) |
| `scheduler.pid` | PID of the running headless process |
| `scheduler.log` | Persistent log file written by the headless service |
| `watchdog.pid` | PID of the watchdog process (present while scheduler runs) |
