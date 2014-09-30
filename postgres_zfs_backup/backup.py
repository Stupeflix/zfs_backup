from postgres_zfs_backup import settings
from postgres_zfs_backup.utils import command, parse_snapshot, new_snapshot_name
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

    def _list_snapshots(self):
        p = command(
            cmd='sudo zfs list -r -t snapshot -o name,creation %s' % settings.POSTGRES_FS,
            check=True)

        for line in p.stdout:
            snapshot, date = parse_snapshot(line)
            if snapshot is not None and date is not None:
                yield snapshot, date

    def stream_last_snapshot(self):
        snapshot, date = self.get_last_snapshot()
        p = command(
            cmd='sudo zfs send %s' % snapshot)
        return snapshot, p.stdout

    def get_last_snapshot(self):
        p = command(
            cmd='sudo zfs list -r -t snapshot -o name,creation %s | tail -n 1' % settings.POSTGRES_FS,
            check=True)

        snapshot = None
        date = None
        lines = p.stdout.readlines()
        if len(lines) > 0:
            snapshot, date = parse_snapshot(lines[0])
        return snapshot, date

    def create(self):
        snapshot = new_snapshot_name()

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
