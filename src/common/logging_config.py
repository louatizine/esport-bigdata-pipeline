"""Central logging configuration loader for project modules.

Load this early in any CLI/app entrypoint to standardize logging.
"""

from __future__ import annotations

import logging
import logging.config
import os
import yaml


def configure_logging(default_path: str = "conf/logging.yaml") -> None:
    """Configure Python logging from YAML file.

    Parameters
    ----------
    default_path: str
        Relative path to the logging YAML (from repo root when running in Codespaces/Compose).
    """
    if not os.path.exists(default_path):
        logging.basicConfig(level=logging.INFO)
        logging.getLogger(__name__).warning("Logging config %s not found; using basicConfig.", default_path)
        return

    with open(default_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    logging.config.dictConfig(config)
