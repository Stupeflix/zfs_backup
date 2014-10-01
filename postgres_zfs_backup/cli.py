from postgres_zfs_backup.backup import Backup
from postgres_zfs_backup.bucket import Bucket
from postgres_zfs_backup import settings

from datetime import datetime, timedelta
import click
import time

import logging
logger = logging.getLogger(__name__)


@click.command()
@click.version_option(settings.VERSION)
def cli(enable_push, hot_backup):
    logger.info('Initializing...')

    backup = Backup(hot_backup=hot_backup)
    snapshot, last_backup = backup.get_last_snapshot()

    if last_backup is None:
        logger.info('No snapshot found')
    else:
        logger.info('Last snapshot: %s (%s)' % (snapshot, last_backup))

    if hasattr(settings, 'BUCKET_CONF'):
        bucket = Bucket(settings.BUCKET_CONF)
        key_name, last_push = bucket.get_last_key()

    while True:
        # Create / cleanup backups
        limit = datetime.now() - timedelta(seconds=settings.SNAPSHOT_INTERVAL)
        if last_backup is None or last_backup <= limit:
            last_backup = datetime.now()
            backup.create()
        backup.cleanup_old_snapshots()

        # Create / cleanup pushed backups
        if hasattr(settings, 'BUCKET_CONF'):
            limit = datetime.now() - timedelta(seconds=settings.BUCKET_CONF['PUSH_INTERVAL'])
            if last_push is None or last_push <= limit:
                last_push = datetime.now()
                snapshot_name, stream = backup.stream_last_snapshot()
                bucket.push(snapshot_name, stream)
            bucket.cleanup_old_keys()

        time.sleep(20)


def main():
    try:
        cli()
    except Exception as e:
        logger.error(e, exc_info=True)

if __name__ == '__main__':
    main()
