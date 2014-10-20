from zfs_backup.snapshot import PostgresSnapshot, PostgresHotSnapshot, MysqlSnapshot
from zfs_backup import settings

import click
import time

import logging
logger = logging.getLogger(__name__)


@click.command()
@click.version_option(settings.VERSION)
def cli():
    logger.info('Initializing...')
    snapshots = []

    for conf in settings.SNAPSHOTS:
        if conf['TYPE'] == 'postgres':
            snapshot = PostgresSnapshot(conf)
        elif conf['TYPE'] == 'postgres-hot':
            snapshot = PostgresHotSnapshot(conf)
        elif conf['TYPE'] == 'mysql':
            snapshot = MysqlSnapshot(conf)
        else:
            continue
        snapshots.append(snapshot)

    while True:
        for snapshot in snapshots:
            snapshot.try_to_create()
            snapshot.cleanup_old_snapshots()

            bucket = snapshot.bucket
            if bucket is not None:
                bucket.try_to_push()
                bucket.cleanup_old_keys()

        time.sleep(20)


def main():
    try:
        cli()
    except Exception as e:
        logger.error(e, exc_info=True)

if __name__ == '__main__':
    main()
