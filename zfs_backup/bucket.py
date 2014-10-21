from zfs_backup import utils
from zfs_backup import settings

from datetime import datetime, timedelta
from subprocess import PIPE
import os
import sys
import boto
import json
import cStringIO
import logging
logger = logging.getLogger(__name__)


class Bucket(object):

    def __init__(self, settings):
        self.settings = settings
        self.conn = boto.connect_s3(
            self.settings['ACCESS_KEY_ID'],
            self.settings['SECRET_ACCESS_KEY'])
        self.bucket = self.conn.get_bucket(self.settings['BUCKET'])

    def terminate(self):
        if hasattr(self, '_uploaded'):
            # Let the process to terminate
            logger.info('Upload is almost complete, terminating...')
            pass
        else:
            self._terminating = True

            # Cancel upload if needed
            if hasattr(self, 'uploader'):
                self.uploader.cancel_upload()
                logger.info('Upload cancelled.')

    def push(self, snapshot, stream):
        # Cut the first part of the snapshot name
        key_name = ('%s:%s.gzip' % (settings.HOSTNAME, snapshot)).replace('/', '_')

        # Gzip the stream
        p = utils.command(
            cmd='gzip -fc',
            stdin=stream)
        stream = p.stdout

        # start_time = datetime.now()
        # logger.info('Pushing key %s...' % key_name)
        self.uploader = self.bucket.initiate_multipart_upload(
            key_name=key_name)

        chunk = stream.read(self.settings['CHUNK_SIZE'])
        part_num = 1
        while chunk:
            fp = cStringIO.StringIO(chunk)
            self.uploader.upload_part_from_file(fp, part_num=part_num)
            fp.seek(0, os.SEEK_END)
            # logger.info('Pushed %s to S3' % utils.filesizeformat(fp.tell()))
            chunk = stream.read(self.settings['CHUNK_SIZE'])
            part_num += 1

        self._uploaded = True
        if not hasattr(self, '_terminating'):
            self.uploader.complete_upload()
            # logger.info('Pushed key %s in %ss' % (key_name, utils.total_seconds(datetime.now() - start_time)))

    def create_job(self, snapshot_name):
        executable = sys.executable
        executable_parts = executable.split('/')
        executable_parts.pop()
        executable = '%s/zfs_backup' % '/'.join(executable_parts)

        p = utils.command(
            '%s send_snapshot --snapshot-name="%s"' % (executable, snapshot_name),
            stdin=PIPE)
        p.stdin.write(json.dumps(self.settings))
        p.stdin.close()

    def remove_key(self, key_name):
        self.bucket.delete_key(key_name)
        logger.info('Deleted key %s' % key_name)


class SnapshotBucket(Bucket):

    def __init__(self, settings, snapshot):
        super(SnapshotBucket, self).__init__(settings)
        self.snapshot = snapshot

    def _list_keys(self):
        prefix = ('%s:%s' % (settings.HOSTNAME, self.snapshot.settings['FILE_SYSTEM'])).replace('/', '_')
        for key in self.bucket.get_all_keys(prefix=prefix):
            snapshot, date = utils.parse_snapshot(key.name.replace('.gzip', ''))
            if snapshot is not None and date is not None:
                yield key.name, date

    @property
    def last_key(self):
        if not hasattr(self, '_last_key'):
            self._last_key = self.get_last_key()
        return self._last_key

    def get_last_key(self):
        last_key = None
        last_date = None
        for key, date in self._list_keys():
            if last_date is None or last_date < date:
                last_key = key
                last_date = date
        if last_key is None:
            return {}
        else:
            return {
                'key': last_key,
                'date': last_date
            }

    def try_to_push(self):
        limit = datetime.now() - timedelta(seconds=self.settings['PUSH_INTERVAL'])
        if 'date' not in self.last_key or self.last_key['date'] <= limit:
            self.last_key['date'] = datetime.now()
            snapshot = self.snapshot.get_last_snapshot()
            self.create_job(snapshot['name'])

    def cleanup_old_keys(self):
        keys = []
        for key_name, date in self._list_keys():
            keys.append({
                'name': key_name,
                'date': date
            })
        if len(keys) > self.settings['MAX_FILES']:
            for i, hit in enumerate(sorted(keys, key=lambda k: k['date'], reverse=True)):
                if i >= self.settings['MAX_FILES']:
                    self.remove_key(hit['name'])
