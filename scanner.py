import datetime
import logging
import os
import time
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
        # Daily sites: generate one extra day so the previous complete day is always included
        extra = 1 if site.frequency == 'daily' else 0
        for day_offset in range(days_back + extra):
            base = now - datetime.timedelta(days=day_offset)
            if site.frequency == 'daily':
                dt = base.replace(hour=0, minute=0, second=0, microsecond=0)
                # Daily file for day D is available at 00:15 on day D+1
                available_dt = (dt + datetime.timedelta(days=1)).replace(hour=0, minute=15)
                fname = dt.strftime(site.pattern)
                expected.append({'dt': dt, 'available_dt': available_dt, 'file': fname, 'date': dt.strftime('%Y-%m-%d')})
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

        if not any(c in site.output_dir for c in ('%', '{')):
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
        now_utc = datetime.datetime.now(timezone.utc)
        path_groups = {}
        fallback_map = {}  # id(exp) -> fallback_path for boundary files

        for exp in expected:
            resolved_path = exp['dt'].strftime(site.path) if has_dynamic_path else site.path
            path_groups.setdefault(resolved_path, []).append(exp)

            # For boundary files with dynamic paths, also search the previous day's
            # directory: last file of a previous day OR first file of any day (hour 00),
            # since GNSS receivers may store these in the preceding day's directory.
            is_previous_day = exp['dt'].date() < now_utc.date()
            is_last_of_prev_day = is_previous_day and (site.frequency == 'daily' or exp['dt'].hour == 23)
            is_first_of_day = site.frequency != 'daily' and exp['dt'].hour == 0
            if has_dynamic_path and (is_last_of_prev_day or is_first_of_day):
                prev_dt = exp['dt'] - datetime.timedelta(days=1)
                fallback_path = prev_dt.strftime(site.path)
                if fallback_path != resolved_path:
                    fallback_map[id(exp)] = fallback_path
                    path_groups.setdefault(fallback_path, [])  # ensure it gets listed

        # List each unique remote path once (includes fallback paths)
        remote_data = {}   # path -> (remote_set, sizes, connection_failed)
        dot_a_first = {}   # path -> {base_fname: size}  (first snapshot of .A files)

        for remote_path, exps in path_groups.items():
            if exps and file_cb:
                file_cb(site.name, f"connecting to {remote_path} ...")
            site_copy = SiteConfig.from_dict(site.to_dict())
            site_copy.path = remote_path
            remote_files, remote_sizes = connector.list_and_size(site_copy)
            failed = remote_files is None
            if remote_files:
                dot_a = {f[:-2]: (remote_sizes or {}).get(f, 0)
                         for f in remote_files if f.endswith('.A')}
                if dot_a:
                    dot_a_first[remote_path] = dot_a
                remote_files = [f for f in remote_files if not f.endswith('.A')]
                remote_sizes = {f: s for f, s in (remote_sizes or {}).items() if not f.endswith('.A')}
            remote_data[remote_path] = (set(remote_files or []), remote_sizes or {}, failed)

        # For paths with .A files, wait briefly then re-check to detect growth
        growing_files = {}  # path -> set of base_fnames whose .A grew
        if dot_a_first:
            if file_cb:
                file_cb(site.name, "checking upload progress...")
            time.sleep(5)
            for remote_path, dot_a_sizes1 in dot_a_first.items():
                site_copy = SiteConfig.from_dict(site.to_dict())
                site_copy.path = remote_path
                _, sizes2 = connector.list_and_size(site_copy)
                if sizes2:
                    growing = {base for base, sz1 in dot_a_sizes1.items()
                               if sizes2.get(base + '.A', sz1) > sz1}
                    if growing:
                        growing_files[remote_path] = growing

        results = []
        for remote_path, exps in path_groups.items():
            if not exps:
                continue
            remote_set, remote_sizes, connection_failed = remote_data[remote_path]

            for exp in exps:
                fname = exp['file']
                if file_cb:
                    file_cb(site.name, fname)
                if any(c in site.output_dir for c in ('%', '{')):
                    local_dir = exp['dt'].strftime(site.output_dir)
                else:
                    local_dir = site.output_dir
                local_path = os.path.join(local_dir, fname)
                local_exists = os.path.exists(local_path)
                local_size = os.path.getsize(local_path) if local_exists else 0
                remote_exists = fname in remote_set
                remote_size = remote_sizes.get(fname, 0)
                actual_remote_path = remote_path

                # If not found in primary path, check the fallback (previous day's dir)
                if not remote_exists and not connection_failed:
                    fallback_path = fallback_map.get(id(exp))
                    if fallback_path and fallback_path in remote_data:
                        fb_set, fb_sizes, fb_failed = remote_data[fallback_path]
                        if not fb_failed and fname in fb_set:
                            remote_exists = True
                            remote_size = fb_sizes.get(fname, 0)
                            actual_remote_path = fallback_path

                size_match = local_exists and remote_exists and local_size == remote_size
                is_future = exp.get('available_dt', exp['dt']) > now_utc

                is_current_utc = False
                if site.frequency == 'daily':
                    is_current_utc = (exp['dt'].date() == now_utc.date())
                elif ' ' in exp['date']:
                    file_date, file_hour = exp['date'].split()
                    current_date = now_utc.strftime("%Y-%m-%d")
                    current_hour = now_utc.strftime("%H")
                    is_current_utc = (file_date == current_date and file_hour.startswith(current_hour))

                # Current-day daily files are "now", not future
                if is_current_utc:
                    is_future = False

                is_uploading = (not remote_exists and not connection_failed and
                                fname in growing_files.get(actual_remote_path, set()))

                if connection_failed and not local_exists:
                    status = 'connection failed'
                elif connection_failed and local_exists:
                    status = 'ok (offline)'
                else:
                    status = ('scheduled' if is_future else
                              'new' if is_current_utc and (remote_exists or is_uploading) else
                              'uploading' if is_uploading else
                              'missing remotely' if not remote_exists else
                              'missing locally' if not local_exists else
                              'size mismatch' if not size_match else 'ok')

                results.append({
                    'site': site.name, 'date': exp['date'], 'file': fname, 'site_obj': site,
                    'local': 'yes' if local_exists else 'no', 'remote': 'yes' if remote_exists else 'no',
                    'local_size': local_size, 'remote_size': remote_size,
                    'size_ok': 'yes' if size_match else 'no', 'status': status,
                    'future': is_future, 'is_current_utc': is_current_utc,
                    'local_path': local_path, 'remote_path': actual_remote_path,
                    'available_dt': exp.get('available_dt', exp['dt']),
                })
        return results
