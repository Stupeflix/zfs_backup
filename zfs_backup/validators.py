import click
from zfs_backup import utils


def zfs_snapshot(ctx, param, value):
    p = utils.command(
        'zfs list -t snapshot -o name | grep %s' % value,
        check=True)
    if p.returncode != 0:
        raise click.BadParameter('zfs snapshot does not exist')
    return value
