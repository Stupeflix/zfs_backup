from postgres_zfs_backup import settings
from subprocess import Popen, PIPE

from datetime import datetime

import math


def command(cmd, check=False, **params):
    p = Popen(
        [cmd],
        shell=True,
        stdout=PIPE,
        stderr=PIPE,
        **params
    )

    if check is True:
        p.wait()
        if p.returncode != 0:
            raise Exception('Command fail', {
                'error': p.stderr.read()
            })

    return p


def new_snapshot_name():
    return '%s%s%s' % (
        settings.FILE_SYSTEM,
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
