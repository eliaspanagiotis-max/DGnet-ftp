import datetime
import logging
import os
from datetime import timezone
from typing import List, Dict
from models import SiteConfig
from connectors import ConnectorFactory

logger = logging.getLogger(__name__)

class FilePatternGenerator:
    @staticmethod
    def generate(site: SiteConfig, days_back: int) -> List[Dict]:
        expected = []
        now = datetime.datetime.now(timezone.utc)
        for day_offset in range(days_back):
            base = now - datetime.timedelta(days=day_offset)
            if site.frequency == 'daily':
                dt = base.replace(hour=0, minute=0, second=0, microsecond=0)
                fname = dt.strftime(site.pattern)
                expected.append({'dt': dt, 'file': fname, 'date': dt.strftime('%Y-%m-%d')})
            else:
                for hour in range(24):
                    dt = base.replace(hour=hour, minute=0, second=0, microsecond=0)
                    pattern = site.pattern.replace('%H', chr(97 + hour)) if site.use_letter_hour else site.pattern
                    fname = dt.strftime(pattern)
                    expected.append({'dt': dt, 'file': fname, 'date': dt.strftime('%Y-%m-%d %H:00')})
        return expected

class SiteScanner:
    def scan_site_all_remote(self, site: SiteConfig, file_cb=None) -> List[Dict]:
        """List every file on the remote server and compare to local storage."""
        connector = ConnectorFactory.get(site.protocol)
        all_files = connector.list_all_recursive(site)

        if all_files is None:
            return [{'site': site.name, 'date': '—', 'file': '(connection failed)',
                     'site_obj': site, 'local': 'no', 'remote': 'no',
                     'local_size': 0, 'remote_size': 0, 'size_ok': 'no',
                     'status': 'connection failed', 'future': False,
                     'is_current_utc': False, 'local_path': '', 'remote_path': site.path}]

        os.makedirs(site.output_dir, exist_ok=True)
        results = []
        for remote_path, fname, remote_size in all_files:
            if file_cb:
                file_cb(site.name, fname)
            local_path = os.path.join(site.output_dir, fname)
            local_exists = os.path.exists(local_path)
            local_size = os.path.getsize(local_path) if local_exists else 0
            size_match = local_exists and local_size == remote_size
            status = ('missing locally' if not local_exists else
                      'size mismatch' if not size_match else 'ok')
            results.append({
                'site': site.name, 'date': '—', 'file': fname, 'site_obj': site,
                'local': 'yes' if local_exists else 'no', 'remote': 'yes',
                'local_size': local_size, 'remote_size': remote_size,
                'size_ok': 'yes' if size_match else 'no', 'status': status,
                'future': False, 'is_current_utc': False,
                'local_path': local_path, 'remote_path': remote_path,
            })
        return results

    def scan_site(self, site: SiteConfig, days_back: int, file_cb=None) -> List[Dict]:
        expected = FilePatternGenerator.generate(site, days_back)
        connector = ConnectorFactory.get(site.protocol)

        # Group expected files by resolved remote path (supports strftime in path)
        has_dynamic_path = any(c in site.path for c in ('%', '{'))
        path_groups = {}
        for exp in expected:
            if has_dynamic_path:
                resolved_path = exp['dt'].strftime(site.path)
            else:
                resolved_path = site.path
            path_groups.setdefault(resolved_path, []).append(exp)

        os.makedirs(site.output_dir, exist_ok=True)
        now_utc = datetime.datetime.now(timezone.utc)
        results = []

        for remote_path, exps in path_groups.items():
            if file_cb:
                file_cb(site.name, f"connecting to {remote_path} ...")
            # List remote files once per unique resolved path
            site_copy = SiteConfig.from_dict(site.to_dict())
            site_copy.path = remote_path
            remote_files, remote_sizes = connector.list_and_size(site_copy)
            connection_failed = remote_files is None
            if connection_failed:
                remote_files, remote_sizes = [], {}
            remote_set = set(remote_files)

            for exp in exps:
                fname = exp['file']
                if file_cb:
                    file_cb(site.name, fname)
                local_path = os.path.join(site.output_dir, fname)
                local_exists = os.path.exists(local_path)
                local_size = os.path.getsize(local_path) if local_exists else 0
                remote_exists = fname in remote_set
                remote_size = remote_sizes.get(fname, 0)
                size_match = local_exists and remote_exists and local_size == remote_size
                is_future = exp['dt'] > now_utc

                is_current_utc = False
                if ' ' in exp['date']:
                    file_date, file_hour = exp['date'].split()
                    current_date = now_utc.strftime("%Y-%m-%d")
                    current_hour = now_utc.strftime("%H")
                    is_current_utc = (file_date == current_date and file_hour.startswith(current_hour))

                if connection_failed and not local_exists:
                    status = 'connection failed'
                elif connection_failed and local_exists:
                    status = 'ok (offline)'
                else:
                    status = ('scheduled' if is_future else
                              'new' if is_current_utc and remote_exists else
                              'missing remotely' if not remote_exists else
                              'missing locally' if not local_exists else
                              'size mismatch' if not size_match else 'ok')

                results.append({
                    'site': site.name, 'date': exp['date'], 'file': fname, 'site_obj': site,
                    'local': 'yes' if local_exists else 'no', 'remote': 'yes' if remote_exists else 'no',
                    'local_size': local_size, 'remote_size': remote_size,
                    'size_ok': 'yes' if size_match else 'no', 'status': status,
                    'future': is_future, 'is_current_utc': is_current_utc,
                    'local_path': local_path, 'remote_path': remote_path
                })
        return results
