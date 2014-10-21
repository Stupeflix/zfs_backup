from zfs_backup import settings
from subprocess import Popen, PIPE

from datetime import datetime

import math


def command(cmd, check=False, **usr_params):
    # Set default params
    params = dict({
        'shell': True,
        'stdout': PIPE,
        'stderr': PIPE,
    }, **usr_params)

    p = Popen(
        [cmd],
        **params
    )

    if check is True:
        p.wait()
        if p.returncode != 0:
            raise Exception('Command fail', {
                'error': p.stderr.read()
            })

    return p


def new_snapshot_name(fs):
    return '%s@%s%s' % (
        fs,
        settings.SNAPSHOT_PREFIX,
        datetime.now().strftime(settings.SNAPSHOT_DATE_FORMAT))


def parse_snapshot(line):
    parts = line.strip().split()
    if len(parts) > 0:
        snapshot = parts.pop(0)
        date = snapshot.split(settings.SNAPSHOT_PREFIX)[-1]
        try:
            date = datetime.strptime(date, settings.SNAPSHOT_DATE_FORMAT)
        except ValueError:
            pass
        else:
            return snapshot, date
    return None, None


def stream_snapshot(snapshot_name):
    return command(
        cmd='sudo zfs send %s' % snapshot_name).stdout


def filesizeformat(bytes, precision=2):
    """Returns a humanized string for a given amount of bytes"""
    bytes = int(bytes)
    if bytes is 0:
        return '0B'
    log = math.floor(math.log(bytes, 1024))
    return "%.*f%s" % (
        precision,
        bytes / math.pow(1024, log),
        ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
        [int(log)]
    )


def total_seconds(td):
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6


def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)
    wrapper.has_run = False
    return wrapper
