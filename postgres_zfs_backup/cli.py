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
@click.option(
    '--enable-push',
    is_flag=True)
@click.option(
    '--hot-backup',
    is_flag=True)
def cli(enable_push, hot_backup):
    logger.info('Initializing...')

    backup = Backup(hot_backup=hot_backup)
    snapshot, last_backup = backup.get_last_snapshot()

    if last_backup is None:
        logger.info('No snapshot found')
    else:
        logger.info('Last snapshot: %s (%s)' % (snapshot, last_backup))

    if enable_push is True:
        bucket = Bucket()
        key_name, last_push = bucket.get_last_key()

    while True:
        # Create / cleanup backups
        if last_backup is None or last_backup <= datetime.now() - timedelta(seconds=settings.BACKUP_INTERVAL):
            last_backup = datetime.now()
            backup.create()
        backup.cleanup_old_snapshots()

        # Create / cleanup pushed backups
        if enable_push is True:
            if last_push is None or last_push <= datetime.now() - timedelta(seconds=settings.PUSH_BACKUP_INTERVAL):
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
