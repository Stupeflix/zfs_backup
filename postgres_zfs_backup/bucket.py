from postgres_zfs_backup import settings
from postgres_zfs_backup.utils import parse_snapshot, command

import boto
import logging
logger = logging.getLogger(__name__)


class Bucket(object):
    def __init__(self):
        self.conn = boto.connect_s3(
            settings.AWS_ACCESS_KEY_ID,
            settings.AWS_SECRET_ACCESS_KEY)
        self.bucket = self.conn.get_bucket(settings.S3_BUCKET)

    def _list_keys(self):
        for key in self.bucket.get_all_keys():
            snapshot, date = parse_snapshot(key.name.replace('.gzip', ''))
            if snapshot is not None and date is not None:
                yield key.name, date

    def get_last_key(self):
        last_key = None
        last_date = None
        for key, date in self._list_keys():
            if last_date is None or last_date < date:
                last_key = key
                last_date = date
        return last_key, last_date

    def push(self, snapshot, stream):
        # Cut the first part of the snapshot name
        key_name = '@%s.gzip' % snapshot.split('@')[-1]

        # Gzip the stream
        p = command(
            cmd='gzip -fc',
            stdin=stream)

        key = self.bucket.new_key(key_name)
        key.set_contents_from_file(p.stdout)

    def remove_key(self, key_name):
        self.bucket.delete_key(key_name)
        logger.info('Deleted key %s' % key_name)

    def cleanup_old_keys(self):
        keys = []
        for key_name, date in self._list_keys():
            keys.append({
                'name': key_name,
                'date': date
            })
        if len(keys) > settings.MAX_S3_BACKUPS:
            for i, hit in enumerate(sorted(keys, key=lambda k: k['date'], reverse=True)):
                if i >= settings.MAX_S3_BACKUPS:
                    self.remove_key(hit['name'])
