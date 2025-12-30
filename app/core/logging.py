from __future__ import annotations

import logging
import os
from logging.config import dictConfig
from typing import Optional


def _norm_level(v: Optional[str], default: str) -> str:
    s = (v or default).upper().strip()
    return s if s in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"} else default


def init_logging(
    *,
    root_level: str = "INFO",
    app_level: Optional[str] = None,
    third_party_level: str = "WARNING",
) -> None:
    """
    Middle-ground logging:

    - root logger controls generic libs (INFO by default)
    - "app" logger controls *your* code (DEBUG in dev is useful)
    - noisy third-party libs are forced WARNING to avoid unreadable dumps

    Env overrides:
      LOG_ROOT_LEVEL=INFO|DEBUG|...
      LOG_APP_LEVEL=INFO|DEBUG|...
      LOG_THIRD_PARTY_LEVEL=WARNING|INFO|...
    """
    root_lvl = _norm_level(os.getenv("LOG_ROOT_LEVEL"), root_level)
    app_lvl = _norm_level(os.getenv("LOG_APP_LEVEL"), app_level or root_lvl)
    third_lvl = _norm_level(os.getenv("LOG_THIRD_PARTY_LEVEL"), third_party_level)

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                    "formatter": "standard",
                }
            },
            "root": {"level": root_lvl, "handlers": ["console"]},
            "loggers": {
                # Your code: chatty + useful
                "app": {"level": app_lvl, "handlers": ["console"], "propagate": False},

                # Uvicorn
                "uvicorn": {"level": root_lvl, "handlers": ["console"], "propagate": False},
                "uvicorn.error": {"level": root_lvl, "handlers": ["console"], "propagate": False},
                "uvicorn.access": {"level": root_lvl, "handlers": ["console"], "propagate": False},

                # Noisy libs: keep quiet unless something is wrong
                "pymongo": {"level": third_lvl, "handlers": ["console"], "propagate": False},
                "httpcore": {"level": third_lvl, "handlers": ["console"], "propagate": False},
                "httpx": {"level": third_lvl, "handlers": ["console"], "propagate": False},
            },
        }
    )

    logging.getLogger(__name__).info(
        "[logging] configured root=%s app=%s third_party=%s",
        root_lvl,
        app_lvl,
        third_lvl,
    )

