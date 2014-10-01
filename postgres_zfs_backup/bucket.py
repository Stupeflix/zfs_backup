from postgres_zfs_backup.utils import parse_snapshot, command, filesizeformat

import boto
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
        stream = p.stdout

        logger.info('Pushing key %s...' % key_name)
        uploader = self.bucket.initiate_multipart_upload(
            key_name=key_name)

        chunk = stream.read(self.settings['CHUNK_SIZE'])
        part_num = 1
        while chunk:
            fp = cStringIO.StringIO(chunk)
            uploader.upload_part_from_file(fp, part_num=part_num)
            logger.info('Pushed %s to S3' % filesizeformat(self.settings['CHUNK_SIZE']))
            chunk = stream.read(self.settings['CHUNK_SIZE'])
            part_num += 1

        uploader.complete_upload()
        logger.info('Pushed key %s' % key_name)

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
        if len(keys) > self.settings['MAX_FILES']:
            for i, hit in enumerate(sorted(keys, key=lambda k: k['date'], reverse=True)):
                if i >= self.settings['MAX_FILES']:
                    self.remove_key(hit['name'])
