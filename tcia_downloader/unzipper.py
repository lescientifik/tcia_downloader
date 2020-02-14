from zipfile import ZipFile
import pathlib
import logging
import os


logger = logging.getLogger(__name__)


def unzip(path: pathlib.Path, dest: str):
    with ZipFile(path, "r") as archive:
        archive.extractall(dest)
    yield path


def remove(path: pathlib.Path):
    os.remove(path)
