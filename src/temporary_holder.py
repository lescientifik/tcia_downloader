import pathlib
import tempfile

import SimpleITK as sitk


def concat_gen(*gens):
    for gen in gens:
        yield from gen


def find_series_id(reader: sitk.ImageSeriesReader, directory: str):
    for identifier in reader.GetGDCMSeriesIDs(directory):
        yield identifier


def list_files_from_tmp_dir(tmp_dir: tempfile.TemporaryDirectory):
    for file in pathlib.Path(tmp_dir.name).rglob("*"):
        yield file
