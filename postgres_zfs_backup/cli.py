from postgres_zfs_backup.backup import Backup
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
    last_cleanup = None

    if last_backup is None:
        logger.info('No snapshot found')
    else:
        logger.info('Last snapshot: %s (%s)' % (snapshot, last_backup))

    while True:
        if last_backup is None or last_backup <= datetime.now() - timedelta(seconds=settings.BACKUP_INTERVAL):
            last_backup = datetime.now()
            backup.create()

        if last_cleanup is None or last_cleanup <= datetime.now() - timedelta(seconds=30):
            last_cleanup = datetime.now()
            backup.cleanup_old_snapshots()

        time.sleep(1)


def main():
    try:
        cli()
    except Exception as e:
        logger.error(e, exc_info=True)

if __name__ == '__main__':
    main()
