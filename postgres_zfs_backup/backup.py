from postgres_zfs_backup import settings
from postgres_zfs_backup import utils
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)


class Backup(object):

    def __init__(self, backup_method):
        self.hot_backup = backup_method == 'hot'

    def _build_cmd(self, cmd):
        if self.hot_backup:
            pre = 'sudo -u postgres psql -c "select pg_start_backup(\'backup\');"'
            post = 'sudo -u postgres psql -c "select pg_stop_backup();"'
        else:
            pre = 'sudo service postgresql stop'
            post = 'sudo service postgresql start'
        return '%s && %s && %s' % (pre, cmd, post)

    def _list_snapshots(self):
        p = utils.command(
            cmd='sudo zfs list -r -t snapshot -o name,creation %s' % settings.FILE_SYSTEM,
            check=True)

        for line in p.stdout:
            snapshot, date = utils.parse_snapshot(line)
            if snapshot is not None and date is not None:
                yield snapshot, date

    def stream_last_snapshot(self):
        snapshot, date = self.get_last_snapshot()
        p = utils.command(
            cmd='sudo zfs send %s' % snapshot)
        return snapshot, p.stdout

    def get_last_snapshot(self):
        p = utils.command(
            cmd='sudo zfs list -r -t snapshot -o name,creation %s | tail -n 1' % settings.FILE_SYSTEM,
            check=True)

        snapshot = None
        date = None
        lines = p.stdout.readlines()
        if len(lines) > 0:
            snapshot, date = utils.parse_snapshot(lines[0])
        return snapshot, date

    def create(self):
        snapshot = utils.new_snapshot_name()

        utils.command(
            cmd=self._build_cmd('sudo zfs snapshot %s' % snapshot),
            check=True)
        logger.info('Created snapshot %s' % snapshot)

    def remove_snapshot(self, snapshot):
        utils.command(
            cmd='sudo zfs destroy %s' % snapshot,
            check=True)
        logger.info('Destroyed snapshot %s' % snapshot)

    def cleanup_old_snapshots(self):
        limit = datetime.now() - timedelta(seconds=settings.SNAPSHOT_MAX_AGE)
        for snapshot, date in self._list_snapshots():
            if date < limit:
                self.remove_snapshot(snapshot)
