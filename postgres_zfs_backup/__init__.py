import postgres_zfs_backup
import logging.config
import logutils.dictconfig
import imp
import os

# -----------------------------------------------------------------------------
# Set app settings to postgres_zfs_backup.settings endpoint

SETTINGS_PATH = os.environ.get('SETTINGS_PATH', '/opt/postgres_zfs_backup/settings.py')
if not hasattr(postgres_zfs_backup, 'settings'):
    postgres_zfs_backup.settings = imp.load_source(
        'postgres_zfs_backup.settings',
        SETTINGS_PATH)
from postgres_zfs_backup import settings


settings.CURRENT_DIR = os.path.dirname(__file__)
settings.VERSION = open(os.path.join(
    settings.CURRENT_DIR,
    'VERSION.txt')
).read()

# -----------------------------------------------------------------------------
# Configure logging

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
            "handlers": ["console"],
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

# Use logutils package if python<2.7
if hasattr(logging.config, 'dictConfig'):
    cls = logging.config
else:
    cls = logutils.dictconfig
cls.dictConfig(settings.LOGGING)
