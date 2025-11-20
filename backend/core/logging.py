"""Configuración centralizada de logging"""

import logging
import logging.config
from core.config import config


def setup_logging():
    """Configura el logging para toda la aplicación"""

    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
            "simple": {"format": "%(levelname)s - %(message)s"},
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": config.log_level,
                "formatter": "simple" if config.log_format == "simple" else "detailed",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "INFO",
                "formatter": "detailed",
                "filename": "/var/log/dfs/system.log",
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
            },
        },
        "loggers": {
            "dfs": {
                "level": config.log_level,
                "handlers": ["console"],
                "propagate": False,
            }
        },
        "root": {"level": "INFO", "handlers": ["console"]},
    }

    logging.config.dictConfig(log_config)
