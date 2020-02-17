import os
import pathlib
import shutil
import sys
import tempfile
import zipfile
from itertools import dropwhile
from typing import BinaryIO, Dict, Iterable, Union

import pydicom
import requests
import SimpleITK as sitk

FilePath = Union[str, os.PathLike]


# source


def read_txt(filepath: FilePath) -> str:
    """Extract lines from file.

    This is a generator encapsulation for reading lines in a file.
    It uses a context manager internally so all cleaning operations will be done
    correctly.

    Parameters
    ----------
    filepath : str or os.PathLike compatible object
        The path for


    Yields
    -------
    str
        A line
    """
    with open(filepath, "r") as file:
        for line in file:
            yield line


def remove_trailing_n(line: str) -> str:
    return line.rstrip("\n")


tcia_endpoint = (
    "https://services.cancerimagingarchive.net/services/v3/TCIA/query/getImage"
)
take_after = "ListOfSeriesToDownload="


def download(
    session: requests.session, endpoint: str, params: Dict
) -> requests.Response:
    """Download a file using requests.

    See https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
    Keeping the same session object for the same endpoint could lead to improved
    performance.
    See https://stackoverflow.com/questions/24873927/python-requests-module-and-connection-reuse
    for more information.

    Parameters
    ----------
    session : requests.session
        The session Object. It is explicitly passed to this function, as reusing
        the session object could lead to improved performance. Session object is theoretically
        thread-safe.
    endpoint : str
        URL
    params : Dict
        GET parameters

    Returns
    -------
    requests.Response
        The response. Nothing will be downloaded until you access any attribute
        on the object (stream = True internally)
    """
    r = session.get(endpoint, params=params, stream=True)
    return r


def tmp_download(response: requests.Response) -> BinaryIO:
    """Save the downloaded file to a temporary file.

    This function leaves you with an open temporary file. You will be
    responsible for closing it later. Closing this file will destroy it.
    See https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests


    Parameters
    ----------
    response : requests.Response
        The http response containing the binary data

    Returns
    -------
    tempfile.NamedTemporaryFile
        The downloaded file as a temporary named file object
    """
    tmp_file = tempfile.NamedTemporaryFile()
    shutil.copyfileobj(response.raw, tmp_file)
    return tmp_file


def close_file(file: BinaryIO) -> BinaryIO:
    return file.close()


def unzip_file(file: BinaryIO) -> tempfile.TemporaryDirectory:
    """Unzip the given file

    If you provide an instance of tempfile.NamedTemporaryFile, the file will
    be closed, so it will be completely erased from disk.

    Parameters
    ----------
    file : BinaryIO
        The file to uncompress

    Returns
    -------
    tempfile.TemporaryDirectory
        The temporary directory where files have been uncompressed.
        This object has a .cleanup() method that allows you to remove it
        and delete all its content afterwards.
    """
    tmp_dir = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(file, "r") as archive:
        archive.extractall(tmp_dir)
    return tmp_dir


def gen_dicomfiles_grouped_by_series(directory):
    """Read all dicom series from given directory.

    For information, simpleITK uses GDCMImageIO and thus
    systematically applies the rescaleSlope and
    RescaleIntercept, so pixel value are already corrected.
    However, further processing is required for SUV value for PET
    modality

    Parameters
    ----------
    directory : str or pathlib.Path object
        The directory containing all the dicom instances

    Yields
    -------
    sitk Image instance
        Each dicom serie found in the directory

    Raises
    ------
    FileNotFoundError
        When either the directory supplied does not exist or it
        does not contain valid .dcm files.

    Warning
    -------
    This will return only one serie per directory. If it contains more than
    one, one of them only will be returned, without any possibility to
    determistically know which one.
    """
    reader = sitk.ImageSeriesReader()
    directory = str(directory)
    # verify directory exists
    if not pathlib.Path(directory).is_dir():
        raise FileNotFoundError(f"{directory} is not a valid directory path")
    # Verify directory contains valid dicom files
    series_IDs = reader.GetGDCMSeriesIDs(directory)  # return a tuple
    if len(series_IDs) == 0:  # nothing inside the directory
        raise FileNotFoundError(
            f"No valid .dcm file was found in the given directory {directory}"
        )

    filenames_grpby_series = (
        reader.GetGDCMSeriesFileNames(directory, index) for index in series_IDs
    )
    ready_readers = (
        reader.SetFileNames(filenames) for filenames in filenames_grpby_series
    )
    images = (reader.Execute() for reader in ready_readers)
    for image in images:
        yield images


def read_volumes_from_tmp_dir(tmp_dir):
    """Read all dicom series from given directory.

    For information, simpleITK uses GDCMImageIO and thus
    systematically applies the rescaleSlope and
    RescaleIntercept, so pixel value are already corrected.
    However, further processing is required for SUV value for PET
    modality

    Parameters
    ----------
    directory : str or pathlib.Path object
        The directory containing all the dicom instances

    Yields
    -------
    sitk Image instance
        Each dicom serie found in the directory

    Raises
    ------
    FileNotFoundError
        When either the directory supplied does not exist or it
        does not contain valid .dcm files.

    Warning
    -------
    This will return only one serie per directory. If it contains more than
    one, one of them only will be returned, without any possibility to
    determistically know which one.
    """
    reader = sitk.ImageSeriesReader()
    with tmp_dir as dir_:  # will remove tmp_dir at the end of the with block
        series_IDs = reader.GetGDCMSeriesIDs(dir_)  # return a tuple
        if len(series_IDs) == 0:  # nothing inside the directory
            raise FileNotFoundError(
                f"No valid .dcm file was found in the given directory {dir_}"
            )
        filenames_grpby_series = (
            reader.GetGDCMSeriesFileNames(dir_, index) for index in series_IDs
        )
        for filenames in filenames_grpby_series:
            reader.SetFileNames(filenames)
            yield reader.Execute


def get_short_metadata(filename) -> Dict:
    field_names = (
        "StudyDate",
        "StudyTime",
        "Modality",
        "Manufacturer",
        "PatientName",
        "PatientID",
        "PatientSex",
        "PatientAge",
        "PatientSize",
        "PatientWeight",
        "ContrastBolusAgent",
        "SeriesNumber",
        "SliceLocation",
        "StudyInstanceUID",
        "SeriesInstanceUID",
        "SOPInstanceUID",
    )
    with pydicom.dcmread(filename, stop_before_pixels=True) as dcm:
        yield {
            value: (getattr(dcm, value) if hasattr(dcm, value) else None)
            for value in field_names
        }


def write_image(sitk_image, filepath):
    filepath = pathlib.Path(filepath)
    if filepath.exists():
        raise FileExistsError
    sitk.WriteImage(sitk_image, str(filepath))


def metadata_to_filepath(metadata: Dict, template: str) -> str:
    pass


def extract_dcm_metadata(dcm_file):
    pass


def concat_gen(*gens):
    for gen in gens:
        yield from gen


def find_series_id(reader: sitk.ImageSeriesReader, directory: str):
    for identifier in reader.GetGDCMSeriesIDs(directory):
        yield identifier


####################################################################################
##################################### PIPELINE #####################################
####################################################################################*

if __name__ == "__main__":
    lines = read_txt(sys.argv[0])
    lines = (remove_trailing_n(line) for line in lines)
    series_ids = dropwhile(lambda x: x != take_after, lines)
    with requests.session() as sess:
        requests = (
            (tcia_endpoint, {"SeriesInstanceUID": series_id})
            for series_id in series_ids
        )
        responses = (download(sess, *request) for request in requests)
    zips = (tmp_download(response) for response in responses)
    tmp_dirs = (unzip_file(zip_) for zip_ in zips)  # full of .dcm
    images_or_none_nested_generator = (
        read_volumes_from_tmp_dir(tmp_dir) for tmp_dir in tmp_dirs
    )
    images_or_none = concat_gen(images_or_none_nested_generator)
    images = (image for image in images_or_none if image is not None)
