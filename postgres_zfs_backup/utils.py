from postgres_zfs_backup import settings
from subprocess import Popen, PIPE

from datetime import datetime


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
        settings.POSTGRES_FS,
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
