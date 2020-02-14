import argparse
import concurrent.futures
import logging

from tcia_downloader.downloader import tcia_images_download
from tcia_downloader.file_parser import get_series_to_dl, parser
from tcia_downloader.logger import create_main_logger
from tcia_downloader.constants import APP_NAME
from tcia_downloader.unzipper import unzip, remove


def multiprocess(target):

    while True:
        target.send(next())
