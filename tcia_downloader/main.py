from __future__ import absolute_import

import argparse
import logging
import pathlib
import concurrent.futures
from datetime import datetime

from tcia_downloader.downloader import tcia_images_download
from tcia_downloader.file_parser import get_series_to_dl, parser

# pylint: disable=logging-format-interpolation

parser = argparse.ArgumentParser(
    description="Download a list of dicom series from a TCIA manifest file"
)
parser.add_argument(
    "--manifest", "-m", help="The path to the TCIA manifest file to parse"
)
parser.add_argument(
    "--out", "-o", help="The directory to which save the downloaded images"
)


def main():
    args = parser.parse_args()
    # Setup the logger
    logfilename = f"{datetime.now().strftime('%Y%m%d_%H%M')}_log.txt"
    logger = logging.getLogger("TCIA DOWNLOAD")
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)

    logger.info(f"log file created at {pathlib.Path(logfilename).absolute()}")

    # sanity check for dirpath
    dirpath = pathlib.Path(args.out)
    if not dirpath.exists():
        dirpath.mkdir()
        logger.info(f"Creating the directory at {dirpath.absolute()}")
    if dirpath.is_dir():
        logger.info(
            f"Files will be downloaded at {dirpath.absolute()}"
            f"Any existing file with instance_UID.zip patterns will be overwritten"
        )
    else:
        raise ValueError()
    logfile_path = dirpath / logfilename
    file_handler = logging.FileHandler(logfile_path.absolute())
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    # real work
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        future_to_serie = {
            executor.submit(
                tcia_images_download, path=dirpath / f"{serie}.zip", instanceID=serie
            ): serie
            for serie in get_series_to_dl(args.manifest)
        }
        for future in concurrent.futures.as_completed(future_to_serie):
            serie = future_to_serie[future]
    logger.info(f"All files downloaded!")


if __name__ == "__main__":
    main()
