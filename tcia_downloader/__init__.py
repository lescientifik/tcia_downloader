import logging

from tcia_downloader.constants import APP_NAME

logging.getLogger(APP_NAME).addHandler(logging.NullHandler())
