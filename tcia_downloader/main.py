from __future__ import absolute_import

import argparse
import logging
import pathlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from tcia_downloader.downloader import tcia_images_download
from tcia_downloader.file_parser import get_series_to_dl, parser

# pylint: disable=logging-format-interpolation

parser = argparse.ArgumentParser()
parser.add_argument(
    "--manifest", "-m", help="The path to the TCIA manifest file to parse"
)
parser.add_argument(
    "--out", "-o", help="The directory to which save the downloaded images"
)

if __name__ == "__main__":
    args = parser.parse_args()
    # Setup a logfile
    logfile = f"{datetime.now().strftime('%Y%m%d_%H%M')}_log.txt"
    logging.basicConfig(level=logging.DEBUG, filename=logfile)
    logging.info(f"log file created at {pathlib.Path(logfile).absolute()}")

    # sanity check for dirpath
    dirpath = pathlib.Path(args.dir)
    if not dirpath.exists():
        dirpath.mkdir()
        logging.info(f"Creating the directory at {dirpath.absolute()}")
    if dirpath.is_dir():
        logging.info(
            f"Files will be downloaded at {dirpath.absolute()}"
            f"Any existing file with serie_instance_UID.zip patterns will be overwritten"
        )

    else:
        raise ValueError(f"This is not a directory")

    # real work
    with ThreadPoolExecutor(max_workers=6) as executor:
        for serie in get_series_to_dl(args.file):
            logging.info(f"Starting download of serie {serie}")
            future = executor.submit(
                tcia_images_download,
                get_series_to_dl,
                path=dirpath / f"{serie}.zip",
                params={"SeriesInstanceUID": serie},
            )
            future.add_done_callback(
                lambda future: logging.info(
                    f"Download of {serie} finished"  # pylint: disable= cell-var-from-loop
                )
            )
