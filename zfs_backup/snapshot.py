from zfs_backup import settings
from zfs_backup import utils
from zfs_backup.bucket import Bucket
from datetime import datetime, timedelta

import MySQLdb
import logging
logger = logging.getLogger(__name__)


class Snapshot(object):

    def __init__(self, settings):
        self.settings = settings

    @property
    def bucket(self):
        if not hasattr(self, '_bucket'):
            if settings.get('BUCKET') is not None:
                self._bucket = Bucket(settings['BUCKET'], self)
            else:
                self._bucket = None
        return self._bucket

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

    @property
    def last_snapshot(self):
        if not hasattr(self, '_last_snapshot'):
            self._last_snapshot = self.get_last_snapshot()
        return self._last_snapshot

    def get_last_snapshot(self):
        p = utils.command(
            cmd='sudo zfs list -r -t snapshot -o name,creation %s | tail -n 1' % settings.FILE_SYSTEM,
            check=True)

        lines = p.stdout.readlines()
        if len(lines) > 0:
            snapshot, date = utils.parse_snapshot(lines[0])
            return {
                'name': snapshot,
                'date': date
            }
        else:
            return {}

    def create(self):
        snapshot = utils.new_snapshot_name()
        self.last_snapshot['date'] = datetime.now()
        self._create('sudo zfs snapshot %s' % snapshot)
        logger.info('Created snapshot %s' % snapshot)

    def _create(self, cmd):
        utils.command(
            cmd=cmd,
            check=True)

    def try_to_create(self):
        limit = datetime.now() - timedelta(seconds=self.settings.SNAPSHOT_INTERVAL)
        if self.last_snapshot is None or self.last_snapshot['date'] <= limit:
            self.create()

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


class PostgresSnapshot(Snapshot):

    def _create(self, cmd):
        cmd = 'sudo service postgresql stop && %s' % cmd
        cmd = '%s && sudo service postgresql start' % cmd
        utils.command(
            cmd=cmd,
            check=True)


class PostgresHotSnapshot(Snapshot):

    def _create(self, cmd):
        cmd = 'sudo -u postgres psql -c "select pg_start_backup(\'backup\');" && %s' % cmd
        cmd = '%s && sudo -u postgres psql -c "select pg_stop_backup();"' % cmd
        utils.command(
            cmd=cmd,
            check=True)


class MysqlSnapshot(Snapshot):

    def _create(self, cmd):
        dbh = MySQLdb.connect(host='localhost', user='root')
        dbh.query('FLUSH TABLES WITH READ LOCK;')

        utils.command(
            cmd=cmd,
            check=True)

        dbh.query('UNLOCK TABLES;')
        dbh.close()
