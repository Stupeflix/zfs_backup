from zfs_backup.snapshot import create_snapshot_from_conf
from zfs_backup.bucket import Bucket
from zfs_backup import settings
from zfs_backup import validators
from zfs_backup import utils

from datetime import datetime
import psutil
import signal
import click
import time
import json
import sys
import os

import logging
logger = logging.getLogger(__name__)


@click.group()
@click.version_option(settings.VERSION)
def cli():
    pass


@cli.command()
def snapshot_daemon():
    logger.info('Initializing daemon...')

    def soft_exit(*args, **kwargs):
        soft_exit_once()

    @utils.run_once
    def soft_exit_once():
        logger.info('Waiting for children...')
        p = psutil.Process(os.getpid())
        children = p.get_children(recursive=True)
        for child in children:
            os.kill(child.pid, signal.SIGTERM)
            try:
                os.waitpid(child.pid, 0)
            except OSError:
                pass
        os.kill(os.getpid(), signal.SIGKILL)

    signal.signal(signal.SIGTERM, soft_exit)
    signal.signal(signal.SIGINT, soft_exit)
    signal.signal(signal.SIGQUIT, soft_exit)
    signal.signal(signal.SIGHUP, soft_exit)

    # -----

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

        time.sleep(5)


@cli.command()
@click.option(
    '--snapshot-name',
    help='Name of an existing ZFS snapshot.',
    callback=validators.zfs_snapshot)
def send_snapshot(snapshot_name):
    logger.info('Initializing a sender for %s...' % snapshot_name)
    start_time = datetime.now()

    conf = json.loads(sys.stdin.read())
    bucket = Bucket(conf)

    def soft_exit(*args, **kwargs):
        soft_exit_once()

    @utils.run_once
    def soft_exit_once():
        logger.info('Soft exit...')
        bucket.terminate()
        os.kill(os.getpid(), signal.SIGKILL)

    signal.signal(signal.SIGTERM, soft_exit)
    signal.signal(signal.SIGINT, soft_exit)
    signal.signal(signal.SIGQUIT, soft_exit)
    signal.signal(signal.SIGHUP, soft_exit)

    bucket.push(
        snapshot_name,
        utils.stream_snapshot(snapshot_name))

    logger.info('Sender for %s: done in %ss' % (snapshot_name, utils.total_seconds(datetime.now() - start_time)))


def main():
    try:
        cli()
    except Exception as e:
        logger.error(e, exc_info=True)

if __name__ == '__main__':
    main()
