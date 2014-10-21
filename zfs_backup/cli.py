from zfs_backup.snapshot import create_snapshot_from_conf
from zfs_backup.bucket import Bucket
from zfs_backup import settings
from zfs_backup import validators
from zfs_backup import utils

import click
import time
import sys
import json

import logging
logger = logging.getLogger(__name__)


@click.command()
@click.version_option(settings.VERSION)
def cli():
    logger.info('Initializing...')


@cli.command()
def snapshot_daemon():
    snapshots = []

    for conf in settings.SNAPSHOTS:
        snapshot = create_snapshot_from_conf(conf)
        if snapshot is not None:
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


@cli.command()
@click.option(
    '--snapshot-name',
    help='Name of an existing ZFS snapshot.',
    callback=validators.zfs_snapshot)
def send_snapshot(snapshot_name):
    conf = json.loads(sys.stdin.read())
    Bucket(conf).push(
        snapshot_name,
        utils.stream_snapshot(snapshot_name))


def main():
    try:
        cli()
    except Exception as e:
        logger.error(e, exc_info=True)

if __name__ == '__main__':
    main()
