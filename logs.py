"""
LOGGING CONFIGURATION

Configure Python's standard logging system for the ingestion scripts. INFO and
higher-severity events are appended to app.log using UTF-8, with each entry
including a timestamp and severity level. This provides a persistent record of
ingestion progress and errors without replacing logs from earlier runs.
"""

import logging

# Set up the shared logging defaults used by scripts that import this module.
logging.basicConfig(
    level= logging.INFO,
    filename="app.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M:%S",
)