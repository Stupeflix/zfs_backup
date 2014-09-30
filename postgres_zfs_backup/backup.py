from postgres_zfs_backup import settings
from postgres_zfs_backup.utils import command
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)


class Backup(object):
    def __init__(self, hot_backup):
        self.hot_backup = hot_backup

    def _build_cmd(self, cmd):
        if self.hot_backup:
            pre = 'sudo -u postgres psql -c "select pg_start_backup(\"backup\");"'
            post = 'sudo -u postgres psql -c "select pg_stop_backup();"'
        else:
            pre = 'sudo service postgresql stop'
            post = 'sudo service postgresql start'
        return '%s && %s && %s' % (pre, cmd, post)

    def _snapshot_name(self):
        return '%s@autobackup-%s' % (
            settings.POSTGRES_FS,
            datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f'))

    def _parse_snapshot(self, line):
        parts = line.strip().split()
        if len(parts) > 0:
            snapshot = parts.pop(0)
            date = ' '.join(parts)
            try:
                date = datetime.strptime(date, '%a %b %d %H:%M %Y')
            except ValueError:
                pass
            else:
                return snapshot, date
        return None, None

    def _list_snapshots(self):
        p = command(
            cmd='sudo zfs list -r -t snapshot -o name,creation %s' % settings.POSTGRES_FS,
            check=True)

        for line in p.stdout:
            snapshot, date = self._parse_snapshot(line)
            if snapshot is not None and date is not None:
                yield snapshot, date

    def get_last_snapshot(self):
        p = command(
            cmd='sudo zfs list -r -t snapshot -o name,creation %s | tail -n 1' % settings.POSTGRES_FS,
            check=True)

        snapshot = None
        date = None
        lines = p.stdout.readlines()
        if len(lines) > 0:
            snapshot, date = self._parse_snapshot(lines[0])
        return snapshot, date

    def create(self):
        snapshot = self._snapshot_name()
        command(
            cmd=self._build_cmd('sudo zfs snapshot %s' % snapshot),
            check=True)
        logger.info('Created snapshot %s' % snapshot)

    def remove_snapshot(self, snapshot):
        command(
            cmd='sudo zfs destroy %s' % snapshot,
            check=True)
        logger.info('Destroyed snapshot %s' % snapshot)

    def cleanup_old_snapshots(self):
        limit = datetime.now() - timedelta(seconds=settings.MAX_BACKUP_AGE)
        for snapshot, date in self._list_snapshots():
            if date < limit:
                self.remove_snapshot(snapshot)
