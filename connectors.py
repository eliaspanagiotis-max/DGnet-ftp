import ftplib
import logging
from paramiko import Transport, SFTPClient
from models import SiteConfig

logger = logging.getLogger(__name__)


class FTPConnector:
    @staticmethod
    def list_and_size(site):
        ftp = None
        try:
            ftp = ftplib.FTP(site.host, timeout=30)
            ftp.login(site.user, site.password)
            ftp.cwd(site.path)
            files = []
            sizes = {}
            # Try MLSD first (most efficient)
            try:
                for entry in ftp.mlsd():
                    name, facts = entry
                    if facts.get('type') == 'file':
                        files.append(name)
                        sizes[name] = int(facts.get('size', 0))
                return files, sizes
            except ftplib.error_perm:
                pass  # MLSD not supported, fall back to LIST
            # Try LIST (parses sizes from directory listing)
            try:
                lines = []
                ftp.retrlines('LIST', lines.append)
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 9 and not line.startswith('d') and not line.startswith('total'):
                        fname = parts[-1]
                        try:
                            fsize = int(parts[4])
                        except (ValueError, IndexError):
                            fsize = 0
                        files.append(fname)
                        sizes[fname] = fsize
                if files:
                    return files, sizes
            except ftplib.error_perm:
                pass
            # Last resort: NLST + individual SIZE commands
            files = ftp.nlst()
            for f in files:
                try:
                    size = ftp.size(f)
                    sizes[f] = size if size is not None else 0
                except ftplib.error_perm:
                    sizes[f] = 0
            return files, sizes
        except ftplib.all_errors as e:
            logger.warning("FTP list failed for %s: %s", site.host, e)
            return None, None
        finally:
            if ftp:
                try:
                    ftp.quit()
                except ftplib.all_errors:
                    pass

    @staticmethod
    def _list_dir(ftp, path):
        """List a single FTP directory. Returns (subdirs, files) where
        files is a list of (name, size). Tries MLSD then LIST."""
        subdirs = []
        files = []
        try:
            entries = list(ftp.mlsd(path))
            for name, facts in entries:
                entry_type = facts.get('type', '')
                if entry_type == 'dir':
                    subdirs.append(name)
                elif entry_type == 'file':
                    files.append((name, int(facts.get('size', 0))))
            return subdirs, files
        except ftplib.all_errors:
            pass  # MLSD not supported, fall back to LIST

        try:
            lines = []
            ftp.retrlines(f'LIST {path}', lines.append)
            for line in lines:
                if not line or line.startswith('total'):
                    continue
                parts = line.split()
                if len(parts) < 9:
                    continue
                name = parts[-1]
                if line.startswith('d'):
                    subdirs.append(name)
                elif not line.startswith('d'):
                    try:
                        size = int(parts[4])
                    except (ValueError, IndexError):
                        size = 0
                    files.append((name, size))
        except ftplib.all_errors as e:
            logger.debug("Cannot list %s: %s", path, e)

        return subdirs, files

    @staticmethod
    def list_all_recursive(site):
        """List all files recursively from the static base of site.path.
        Returns list of (remote_dir, filename, size) or None on failure."""
        path = site.path
        idx = path.find('%')
        if idx >= 0:
            pre = path[:idx]
            last_slash = pre.rfind('/')
            base_path = pre[:last_slash + 1] if last_slash >= 0 else '/'
        else:
            base_path = path

        ftp = None
        try:
            ftp = ftplib.FTP(site.host, timeout=30)
            ftp.login(site.user, site.password)
            all_files = []
            dirs_to_visit = [base_path.rstrip('/') or '/']

            while dirs_to_visit:
                current = dirs_to_visit.pop(0)
                subdirs, files = FTPConnector._list_dir(ftp, current)
                for name in subdirs:
                    dirs_to_visit.append(f"{current.rstrip('/')}/{name}")
                for name, size in files:
                    all_files.append((current, name, size))

            return all_files
        except ftplib.all_errors as e:
            logger.warning("FTP recursive list failed for %s: %s", site.host, e)
            return None
        finally:
            if ftp:
                try:
                    ftp.quit()
                except ftplib.all_errors:
                    pass

    @staticmethod
    def download(site, fname, local_path):
        ftp = None
        try:
            ftp = ftplib.FTP(site.host, timeout=60)
            ftp.login(site.user, site.password)
            ftp.cwd(site.path)
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {fname}', f.write)
            return True
        except ftplib.all_errors as e:
            logger.warning("FTP download failed for %s/%s: %s", site.host, fname, e)
            return False
        finally:
            if ftp:
                try:
                    ftp.quit()
                except ftplib.all_errors:
                    pass


class SFTPConnector:
    @staticmethod
    def list_and_size(site):
        transport = None
        port = site.port or 22
        try:
            transport = Transport((site.host, port))
            transport.connect(username=site.user, password=site.password)
            sftp = SFTPClient.from_transport(transport)
            sftp.chdir(site.path)
            attrs = sftp.listdir_attr()
            files = [a.filename for a in attrs if a.st_size is not None and a.st_size >= 0]
            sizes = {a.filename: a.st_size for a in attrs}
            sftp.close()
            return files, sizes
        except (OSError, Exception) as e:
            logger.warning("SFTP list failed for %s: %s", site.host, e)
            return None, None
        finally:
            if transport:
                try:
                    transport.close()
                except OSError:
                    pass

    @staticmethod
    def list_all_recursive(site):
        """List all files recursively from the static base of site.path."""
        import stat as stat_mod
        path = site.path
        idx = path.find('%')
        if idx >= 0:
            pre = path[:idx]
            last_slash = pre.rfind('/')
            base_path = pre[:last_slash + 1] if last_slash >= 0 else '/'
        else:
            base_path = path

        transport = None
        port = site.port or 22
        try:
            transport = Transport((site.host, port))
            transport.connect(username=site.user, password=site.password)
            sftp = SFTPClient.from_transport(transport)
            all_files = []
            dirs_to_visit = [base_path.rstrip('/') or '/']

            while dirs_to_visit:
                current = dirs_to_visit.pop(0)
                try:
                    for attr in sftp.listdir_attr(current):
                        entry_path = f"{current.rstrip('/')}/{attr.filename}"
                        if stat_mod.S_ISDIR(attr.st_mode):
                            dirs_to_visit.append(entry_path)
                        else:
                            all_files.append((current, attr.filename, attr.st_size or 0))
                except OSError as e:
                    logger.debug("Cannot list %s: %s", current, e)

            sftp.close()
            return all_files
        except (OSError, Exception) as e:
            logger.warning("SFTP recursive list failed for %s: %s", site.host, e)
            return None
        finally:
            if transport:
                try:
                    transport.close()
                except OSError:
                    pass

    @staticmethod
    def download(site, fname, local_path):
        transport = None
        port = site.port or 22
        try:
            transport = Transport((site.host, port))
            transport.connect(username=site.user, password=site.password)
            sftp = SFTPClient.from_transport(transport)
            remote_path = f"{site.path.rstrip('/')}/{fname}"
            sftp.get(remote_path, local_path)
            sftp.close()
            return True
        except (OSError, Exception) as e:
            logger.warning("SFTP download failed for %s/%s: %s", site.host, fname, e)
            return False
        finally:
            if transport:
                try:
                    transport.close()
                except OSError:
                    pass


class ConnectorFactory:
    @staticmethod
    def get(protocol):
        return FTPConnector if protocol == 'ftp' else SFTPConnector
