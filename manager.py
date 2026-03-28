import json
import logging
import os
from typing import List, Dict, Callable
from models import SiteConfig, MissingFilesLog
from scanner import SiteScanner
from connectors import ConnectorFactory
from config import Config
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class FTPSiteManager:
    def __init__(self):
        self.config = Config()
        self.sites: List[SiteConfig] = []
        self.scanner = SiteScanner()
        self._load_sites()

    def scan_all(self, days_back=1, progress_cb: Callable[[str], None] = None,
                  site_cb: Callable[[str, list], None] = None,
                  file_cb: Callable[[str, str], None] = None) -> MissingFilesLog:
        log = MissingFilesLog()
        for site in self.sites:
            if progress_cb:
                progress_cb(f"Scanning {site.name} [{site.network} {site.rate}]...")
            items = self.scanner.scan_site(site, days_back, file_cb=file_cb)
            log.add(site.name, items)
            if site_cb:
                site_cb(site.name, items)
        if progress_cb:
            progress_cb("Scan complete")
        return log

    def scan_all_remote(self, progress_cb=None, site_cb=None, file_cb=None) -> MissingFilesLog:
        log = MissingFilesLog()
        for site in self.sites:
            if progress_cb:
                progress_cb(f"Scanning all remote files for {site.name}...")
            items = self.scanner.scan_site_all_remote(site, file_cb=file_cb)
            log.add(site.name, items)
            if site_cb:
                site_cb(site.name, items)
        if progress_cb:
            progress_cb("Full remote scan complete")
        return log

    def auto_download_completed(self, log: MissingFilesLog, delay_minutes: int):
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=delay_minutes)
        items = []
        for site_items in log.log.values():
            for item in site_items:
                if item['status'] in ['missing locally', 'size mismatch'] and not item.get('is_current_utc'):
                    try:
                        if ' ' in item['date']:
                            d, t = item['date'].split()
                            file_dt = datetime.strptime(f"{d} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
                        else:
                            file_dt = datetime.strptime(item['date'], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        if file_dt < cutoff:
                            items.append(item)
                    except ValueError:
                        logger.debug("Could not parse date '%s', skipping", item['date'])
        if items:
            self.download_missing(items, lambda msg: logger.info(msg))

    def download_missing(self, items, progress_cb=None):
        from notifier import notify_files_downloaded
        total = len(items)
        downloaded = []
        for i, item in enumerate(items):
            if progress_cb:
                progress_cb(f"Downloading {item['file']} ({i+1}/{total})")
            site = item['site_obj']
            # Use resolved remote_path if available (for dynamic paths)
            if 'remote_path' in item and item['remote_path'] != site.path:
                site = SiteConfig.from_dict(site.to_dict())
                site.path = item['remote_path']
            conn = ConnectorFactory.get(site.protocol)
            os.makedirs(os.path.dirname(item['local_path']), exist_ok=True)
            success = conn.download(site, item['file'], item['local_path'])
            if success and os.path.exists(item['local_path']):
                item['local_size'] = os.path.getsize(item['local_path'])
                item['status'] = 'ok'
                item['local'] = 'yes'
                item['size_ok'] = 'yes'
                downloaded.append(f"[{item['site']}] {item['file']}")
        if downloaded:
            notify_files_downloaded(downloaded)

    def get_last_file_statuses(self, log: MissingFilesLog, delay_minutes: int = 0):
        """Return the most recently expected completed file per site with its download status.

        Files within the scheduler delay window are marked 'pending' rather than missing,
        because the scheduler hasn't had time to download them yet.
        """
        now = datetime.now(timezone.utc)
        delay_cutoff = now - timedelta(minutes=delay_minutes)
        results = []
        for site_name, items in log.log.items():
            completed = [it for it in items
                         if not it.get('future') and not it.get('is_current_utc')]
            if not completed:
                continue
            last = max(completed, key=lambda it: it['date'])
            # Resolve the file's expected datetime for the delay check
            last_dt = last.get('available_dt')
            if not last_dt:
                try:
                    date_str = last['date']
                    fmt = "%Y-%m-%d %H:%M" if ' ' in date_str else "%Y-%m-%d"
                    last_dt = datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
                except (ValueError, KeyError):
                    last_dt = None
            pending = last_dt is not None and last_dt >= delay_cutoff
            status = last['status']
            if status in ('missing locally', 'missing remotely') and pending:
                status = 'pending'
            results.append({
                'site': site_name,
                'file': last['file'],
                'date': last['date'],
                'status': status,
            })
        return results

    def add_site(self, **kw):
        self.sites.append(SiteConfig(**kw))
        self._save()

    def edit_site(self, i, **kw):
        for k, v in kw.items():
            setattr(self.sites[i], k, v)
        self._save()

    def delete_site(self, i):
        del self.sites[i]
        self._save()

    def _save(self):
        with open(self.config.sites_file, 'w') as f:
            json.dump([s.to_dict() for s in self.sites], f, indent=2)

    def _load_sites(self):
        if os.path.exists(self.config.sites_file):
            try:
                with open(self.config.sites_file) as f:
                    data = json.load(f)
                    for d in data:
                        self.sites.append(SiteConfig.from_dict(d))
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.error("Failed to load sites config: %s", e)
