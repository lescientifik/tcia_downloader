import logging
import os
import pathlib
import re
import shutil
import sys
import tempfile
import zipfile
import time
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import BinaryIO, Callable, Dict, List, Union, Generator

import requests
import SimpleITK as sitk

FilePath = Union[str, os.PathLike]

# source


def read_txt(fileobj: BinaryIO) -> str:
    """Extract lines from file.

    This is a generator encapsulation for reading lines in a file.
    It uses a context manager internally so all cleaning operations will be done
    correctly.

    Parameters
    ----------
    filepath : BinaryIO
        An open file

    Yields
    -------
    str
        A line
    """
    for line in fileobj:
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
    r = session.get(endpoint, params=params)
    return r


def tcia_downloader(seriesID):
    tmp_file = tempfile.NamedTemporaryFile()
    with requests.get(
        tcia_endpoint, params={"SeriesInstanceUID": seriesID}, stream=True
    ) as r:
        r.raise_for_status()
        # f = open(tmp_file, "wb")
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:  # filter out keep-alive new chunks
                tmp_file.write(chunk)
        return tmp_file
    # tmp_file = tempfile.NamedTemporaryFile()
    # with requests.get(tcia_endpoint, params={"SeriesInstanceUID": seriesID}) as r:
    #     r.raise_for_status()
    #     logging.debug(seriesID)
    #     tmp_file.write(r.content)
    #     return tmp_file


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
        archive.extractall(tmp_dir.name)
    return tmp_dir


def volume_reader():
    reader = sitk.ImageFileReader()
    reader.SetImageIO("GDCMImageIO")
    return reader


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
    with tmp_dir as directory:  # will remove tmp_dir at the end of the with block
        series_IDs = reader.GetGDCMSeriesIDs(directory)  # return a tuple
        if len(series_IDs) == 0:  # nothing inside the directory
            raise FileNotFoundError(
                f"No valid .dcm file was found in the given directory {directory}"
            )
        filenames_grpby_series = (
            reader.GetGDCMSeriesFileNames(directory, index) for index in series_IDs
        )
        for filenames in filenames_grpby_series:
            reader.SetFileNames(filenames)
            yield reader.Execute()


def write_image(sitk_image, filepath):
    filepath = pathlib.Path(filepath)
    if filepath.exists():
        raise FileExistsError
    sitk.WriteImage(sitk_image, str(filepath))


DataElement = namedtuple("DataElement", ["tag", "name"])
data_elements = [
    ("0008|0020", "Study Date"),
    ("0008|0030", "Study Time"),
    ("0008|1030", "Study Description"),
    ("0020|000d", "Study Instance UID"),
    ("0008|0021", "Series Date"),
    ("0008|0031", "Series Time"),
    ("0008|103e", "Series Description"),
    ("0020|000e", "Series Instance UID"),
    ("0020|0013", "Instance Number"),
    ("0020|1041", "Slice Location"),
    ("0054|0081", "Number of Slices"),
    ("0018|0010", "Contrast/Bolus Agent"),
    ("0010|0010", "Patient's Name"),
    ("0010|0020", "Patient ID"),
    ("0010|0030", "Patient's Birth Date"),
    ("0010|0040", "Patient's Sex"),
    ("0010|1010", "Patient's Age"),
    ("0010|1020", "Patient's Size"),
    ("0010|1030", "Patient's Weight"),
    ("0008|0080", "Institution Name"),
    ("0008|0018", "SOP Instance UID"),
    ("0008|0032", "Acquisition Time"),
    ("0008|0060", "Modality"),
    ("0008|0008", "Image Type"),
    ("0008|0070", "Manufacturer"),
    ("0008|1090", "Manufacturer's Model Name"),
]


data_elements_machine = {tag: name for tag, name in data_elements}
data_elements_human = {name: tag for tag, name in data_elements}

default_elements_machine = {tag: "Unknown" for tag, _ in data_elements}


def metadata_reader():
    reader = sitk.ImageFileReader()
    reader.SetImageIO("GDCMImageIO")
    reader.SetLoadPrivateTags(True)
    return reader


def extract_dcm_metadata(filename: str, reader: Callable):
    file_object = pathlib.Path(filename)
    if not file_object.exists():
        raise FileNotFoundError
    reader.SetFileName(filename)
    reader.ReadImageInformation()
    metadata = {
        key: reader.GetMetaData(key)
        for key in data_elements_machine
        if reader.HasMetaDataKey(key)
    }
    metadata.update(file=file_object)
    metadata = {**default_elements_machine, **metadata}
    return metadata


def mv_dcm(metadata: Dict, base_folder: pathlib.Path) -> None:
    old_path = metadata["file"]
    new_path = base_folder / metadata_to_filepath(metadata)
    if new_path.exists():
        raise FileExistsError(metadata, new_path)
    # roughly equivalent to mv: on Unix, if target exists and is a file,
    #  it will be replaced silently if the user has permission.
    old_path.rename(ensure(new_path))


def get_valid_filepath(s: str) -> str:
    # Adapated from django get_valid_filename function
    # https://github.com/django/django/blob/master/django/utils/text.py
    s = str(s).strip().replace(" ", "_").replace(".", "_")
    return re.sub(r"(?u)[^-\w.]", "", s)


def metadata_to_filepath(metadata: Dict) -> str:
    # Patient_Name_ID/Study_Date_Desc/Series_Desc_Modality/Inst-Number.dcm
    tags = [
        "0010|0010",  # Patient's name
        "0010|0020",  # ID
        "0008|0020",  # Study Date
        "0008|1030",  # Study Desc
        "0020|000d",  # Study ID
        "0008|0060",  # Modality
        "0008|103e",  # Series Desc
        "0020|000e",  # Series ID
        "0020|0013",  # Inst number
    ]
    pieces = [get_valid_filepath(metadata[tag]) for tag in tags]
    patient = "_".join(pieces[:2])
    study = "_".join(pieces[2:4])
    series = "_".join(pieces[5:7])
    instance = pieces[8]
    path = pathlib.Path(patient) / study / series / f"{instance}.dcm"
    return path


def concat_gen(*gens):
    for gen in gens:
        yield from gen


def find_series_id(reader: sitk.ImageSeriesReader, directory: str):
    for identifier in reader.GetGDCMSeriesIDs(directory):
        yield identifier


def list_files_from_tmp_dir(tmp_dir: tempfile.TemporaryDirectory):
    for file in pathlib.Path(tmp_dir.name).rglob("*"):
        yield file


class DirectoryNotEmptyError(Exception):
    pass


def is_empty(directory: pathlib.Path) -> bool:
    return not any(directory.iterdir())


def mkdir_safe(s: str) -> pathlib.Path:
    """Safely create a directory

    If the directory already exists, this function will check if it is empty.
    If not, an exception is raised. Else, you will get back the pathlib.Path object
    pointing to it.
    If parents of the directory does not exists, an exception is raised

    Parameters
    ----------
    s : str
        The path to the directory to create

    Returns
    -------
    pathlib.Path
        The path like object created/checked for emptiness

    Raises
    ------
    DirectoryNotEmptyError
        If the path exists, and is not empty
    """
    path = pathlib.Path(s)
    try:
        path.mkdir()
    except FileExistsError:
        if is_empty(path):
            path.mkdir(exist_ok=True)
            return path
        else:
            raise DirectoryNotEmptyError(path.absolute())
    return path


def classify_and_save_dcm(tmp_dirs: List, dest_folder: pathlib.Path) -> None:
    for dcm_dir in tmp_dirs:
        with dcm_dir as dir_:
            files_obj = (file_obj for file_obj in pathlib.Path(dir_).rglob("*"))
            metadatas = (
                extract_dcm_metadata(str(file_obj), m_reader) for file_obj in files_obj
            )
            for m_data in metadatas:
                mv_dcm(m_data, dest_folder)


def threaded_gen(pool, func, ite, *args, **kwargs):
    results = []
    for i in ite:
        results.append(pool.submit(func, i, *args, **kwargs))
    for result in as_completed(results):
        logging.debug(result.result().name)
        yield result.result()


def drop_until(predicate: Callable, gen: Generator):
    for i in gen:
        if predicate(i):
            break
    for i in gen:
        yield i


def ensure(path: pathlib.Path):
    # https://stackoverflow.com/questions/53027297/way-for-pathlib-path-rename-to-create-intermediate-directories
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


####################################################################################
##################################### PIPELINE #####################################
####################################################################################

if __name__ == "__main__":
    start = time.perf_counter()
    logging.basicConfig(level=logging.DEBUG)
    # setup
    pool = ThreadPoolExecutor(3)  # multiThread
    m_reader = metadata_reader()  # dicom headers' reader
    manifest = pathlib.Path(sys.argv[1])
    dest_folder = mkdir_safe(sys.argv[2])
    # basic checks
    if not all([manifest.exists(), manifest.is_file()]):
        raise ValueError(f"{manifest} does not exist or is not a file")
    open_manifest = manifest.open()

    # processing pipeline
    lines = read_txt(open_manifest)  # manifest file
    lines = (remove_trailing_n(line) for line in lines)
    series_ids = drop_until(lambda x: x == take_after, lines)
    zips = threaded_gen(pool, tcia_downloader, series_ids)
    tmp_dirs = (unzip_file(zip_) for zip_ in zips)  # full of .dcm
    classify_and_save_dcm(tmp_dirs, dest_folder)
    pool.shutdown()
    open_manifest.close()
    logging.debug(str(time.perf_counter() - start))
