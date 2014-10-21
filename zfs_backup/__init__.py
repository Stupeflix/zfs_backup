import zfs_backup
import logging.config
import logutils.dictconfig
import socket
import imp
import os

# -----------------------------------------------------------------------------
# Set app settings to zfs_backup.settings endpoint

SETTINGS_PATH = os.environ.get('SETTINGS_PATH', '/opt/zfs_backup/settings.py')
if not hasattr(zfs_backup, 'settings'):
    zfs_backup.settings = imp.load_source(
        'zfs_backup.settings',
        SETTINGS_PATH)
from zfs_backup import settings


settings.CURRENT_DIR = os.path.dirname(__file__)
settings.VERSION = open(os.path.join(
    settings.CURRENT_DIR,
    'VERSION.txt')
).read()

# -----------------------------------------------------------------------------
# Configure logging

settings.HOSTNAME = socket.gethostname()
settings.SNAPSHOT_PREFIX = 'autobackup-'
settings.SNAPSHOT_DATE_FORMAT = '%Y-%m-%d-%H-%M-%S-%f'
settings.LOG_DIRECTORY = '/var/log/zfs_backup'

if not os.path.exists(settings.LOG_DIRECTORY):
    try:
        os.mkdir(settings.LOG_DIRECTORY)
    except OSError:
        pass

settings.LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,  # this fixes the problem

    'formatters': {
        'standard': {
            'format': '[%(asctime)s][%(levelname)s] %(name)s %(filename)s:%(funcName)s:%(lineno)d | %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
    },
    'loggers': {
        '': {
            "level": "DEBUG",
            "handlers": ["console", "file"],
        },
    },
}

# Handle sentry conf
if hasattr(settings, 'SENTRY_DSN'):
    settings.LOGGING['handlers']['sentry'] = {
        'level': 'ERROR',
        'class': 'raven.handlers.logging.SentryHandler',
        'dsn': settings.SENTRY_DSN,
    }
    settings.LOGGING['loggers']['']['handlers'].append('sentry')

# Handle rotating file
if hasattr(settings, 'ROTATING_FILE'):
    settings.LOGGING['handlers']['file'] = {
        'level': 'INFO',
        'class': 'cloghandler.ConcurrentRotatingFileHandler',
        'formatter': 'standard',
        'filename': os.path.join(settings.LOG_DIRECTORY, settings.ROTATING_FILE),
        'backupCount': 5,
        'maxBytes': 1024 * 1024 * 20
    }
    settings.LOGGING['loggers']['']['handlers'].remove('console')
    settings.LOGGING['loggers']['']['handlers'].append('file')

# Use logutils package if python<2.7
if hasattr(logging.config, 'dictConfig'):
    cls = logging.config
else:
    cls = logutils.dictconfig
cls.dictConfig(settings.LOGGING)
