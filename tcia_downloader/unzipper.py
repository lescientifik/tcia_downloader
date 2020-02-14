from zipfile import ZipFile
import pathlib


def unzip(path: pathlib.Path, dest: str):
    with ZipFile(path, "r") as archive:
        archive.extractall(dest)
