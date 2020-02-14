from __future__ import absolute_import

import argparse
import concurrent.futures
import logging

from tcia_downloader.downloader import tcia_images_download
from tcia_downloader.file_parser import get_series_to_dl, parser
from tcia_downloader.logger import create_main_logger
from tcia_downloader.constants import APP_NAME
from tcia_downloader.unzipper import unzip, remove

# pylint: disable=logging-format-interpolation

logger = logging.getLogger(f"{APP_NAME}.{__name__}")

parser = argparse.ArgumentParser(
    description="Download a list of dicom series from a TCIA manifest file"
)
parser.add_argument(
    "--manifest", "-m", help="The path to the TCIA manifest file to parse"
)
parser.add_argument(
    "--out", "-o", help="The directory to which save the downloaded images"
)


def source(src, first_target):
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        for data in src:
            executor.submit(first_target.send, data)


def multiprocess_source(src):
    for serie in src:
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        future_to_serie = {
            executor.submit(
                tcia_images_download, path=dirpath / f"{serie}.zip", instanceID=serie
            ): serie
            for serie in series
        }



def main():
    args = parser.parse_args()

    dirpath = create_main_logger(args.out)

    # get the IDs generator
    series = get_series_to_dl(args.manifest)
    src = args.manifest

    pipeline = [tcia_images_download, unzip, remove]

    # real work
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        future_to_serie = {
            executor.submit(
                tcia_images_download, path=dirpath / f"{serie}.zip", instanceID=serie
            ): serie
            for serie in series
        }
        concurrent.futures.wait(future_to_serie)
        # for _ in concurrent.futures.as_completed(future_to_serie):
        #     serie = future_to_serie[future]
    logger.info(f"All files downloaded!")


if __name__ == "__main__":
    main()
