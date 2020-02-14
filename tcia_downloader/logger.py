import logging
import pathlib
from datetime import datetime

from tcia_downloader.constants import APP_NAME

# pylint: disable=logging-format-interpolation


def create_main_logger(savepath: str):
    """Create and set up the main library logger.

    It is set to debug by default and will log to file and console.

    Parameters
    ----------
    savepath : str
        The directory to which save the log.

    Returns
    -------
    pathlib.Path
        The argument of the function, transformed to a Path object.

    Raises
    ------
    ValueError
        [description]
    """
    # get the main logger
    logger = logging.getLogger(APP_NAME)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s :: %(levelname)s :: %(name)s :: %(message)s"
    )

    # log to console
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)

    # sanity check for dirpath
    dirpath = pathlib.Path(savepath)
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

    # log to file
    logfilename = f"{datetime.now().strftime('%Y%m%d_%H%M')}_log.txt"
    logfile_path = dirpath / logfilename
    file_handler = logging.FileHandler(logfile_path.absolute())
    logger.info(f"log file created at {logfile_path.absolute()}")

    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return dirpath
