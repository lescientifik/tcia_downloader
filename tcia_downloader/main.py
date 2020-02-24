import logging
import pathlib
import re
import sys
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import IO, Callable, Dict, Generator, Iterable, List, TextIO

import requests
import SimpleITK as sitk

# source
TCIA_ENDPOINT = (
    "https://services.cancerimagingarchive.net/services/v3/TCIA/query/getImage"
)
TAKE_AFTER = "ListOfSeriesToDownload="


def read_txt(fileobj: TextIO) -> str:
    """Extract lines from file.

    This is a generator encapsulation for reading lines in a file.
    It uses a context manager internally so all cleaning operations will be done
    correctly.

    Parameters
    ----------
    filepath : TextIO
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


def tcia_downloader(seriesID: str) -> IO:
    """Download a file using requests.

    TODO
    See https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
    Keeping the same session object for the same endpoint could lead to improved
    performance.
    See https://stackoverflow.com/questions/24873927/python-requests-module-and-connection-reuse
    for more information.

    Parameters
    ----------
    seriesID : str
        The parsed string from manifest.tcia file, giving the serie to download.

    Returns
    -------
    IO (tempfile.NamedTemporaryFile)
        An open temporary file containing the downloaded series (.zip)
    """
    tmp_file = tempfile.NamedTemporaryFile()
    with requests.get(
        TCIA_ENDPOINT, params={"SeriesInstanceUID": seriesID}, stream=True
    ) as r:
        r.raise_for_status()
        # f = open(tmp_file, "wb")
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:  # filter out keep-alive new chunks
                tmp_file.write(chunk)
        return tmp_file


def unzip_file(file: IO) -> tempfile.TemporaryDirectory:
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


def metadata_reader() -> sitk.ImageFileReader:
    """Create a .dcm header reader

    Returns
    -------
    sitk.ImageFileReader
        An Sitk reader, preconfigured to read only .dcm files
        and extract also private Dicom tags.
    """
    reader = sitk.ImageFileReader()
    reader.SetImageIO("GDCMImageIO")
    reader.SetLoadPrivateTags(True)
    return reader


def read_volumes_from_tmp_dir(tmp_dir: tempfile.TemporaryDirectory):
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


def write_image(sitk_image: sitk.Image, filepath: pathlib.Path) -> None:
    """Write A 3D volume to the given destination

    Parameters
    ----------
    sitk_image : sitk.Image
        The volume to write
    filepath : pathlib.Path
        The saving path

    Raises
    ------
    FileExistsError
        If you provide a file path that does exists, this exception will
        be raised.
    """
    filepath = pathlib.Path(filepath)
    if filepath.exists():
        raise FileExistsError
    sitk.WriteImage(sitk_image, str(filepath))


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


def extract_dcm_metadata(filename: str, reader: sitk.ImageFileReader) -> Dict:
    """Extract main informations from .dcm file

    Parameters
    ----------
    filename : str
        The Path to the .dcm file
    reader : sitk.ImageFileReader
        Dicom reader

    Returns
    -------
    Dict
        Vital informations from heaers. An additionnal key is provided containing the path
        to the original .dcm file. Moreover, if one of the to-be-extracted dicom tags
        is not found on the file, its value on the Dict will be "Unknown".

    Raises
    ------
    FileNotFoundError
        If the file path to the .dcm file does not exist.
    """
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
    """Move a dicom file to another location based on the dicom headers metadata

    Parameters
    ----------
    metadata : Dict
        Dicom headers metadata and .dcm file location
    base_folder : pathlib.Path
        The root folder where to drop the file.

    Raises
    ------
    FileExistsError
        If the file already exists.

    Note
    ----
    The root folder will contain the .dcm file, but the .dcm file will not be its direct child.
    """
    old_path = metadata["file"]
    new_path = base_folder / metadata_to_dcm_filepath(metadata)
    if new_path.exists():
        raise FileExistsError(metadata, new_path)
    # roughly equivalent to mv: on Unix, if target exists and is a file,
    #  it will be replaced silently if the user has permission.
    old_path.rename(ensure(new_path))


def get_valid_filepath(s: str) -> str:
    """Convert a string to a safe-to-use string for naming file.

    Parameters
    ----------
    s : str
        The given strings

    Returns
    -------
    str
        The safe string

    Note
    ----
    Adapated from django get_valid_filename function:
    https://github.com/django/django/blob/master/django/utils/text.py
    """
    s = str(s).strip().replace(" ", "_").replace(".", "_")
    return re.sub(r"(?u)[^-\w.]", "", s)


def metadata_to_dcm_filepath(metadata: Dict) -> pathlib.Path:
    """Use Metadata to create a filepath.

    Parameters
    ----------
    metadata : Dict
        The dictionnary containing the metadata used for creating the filepath

    Returns
    -------
    pathlib.Path
        A file path encoding the metadata.
    """
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


def metadata_to_nii_filepath(metadata: Dict) -> pathlib.Path:
    """Use Metadata to create a filepath.

    Parameters
    ----------
    metadata : Dict
        The dictionnary containing the metadata used for creating the filepath

    Returns
    -------
    pathlib.Path
        A file path encoding the metadata.
    """
    path = metadata_to_dcm_filepath(metadata)
    new_path = pathlib.Path(str(path.parent) + ".nii.gz")
    return new_path


class DirectoryNotEmptyError(Exception):
    pass


def is_empty(directory: pathlib.Path) -> bool:
    """Check if the supplied directory is empty.

    Parameters
    ----------
    directory : pathlib.Path
        The directory to check.

    Returns
    -------
    bool
    """
    return not any(directory.iterdir())


def mkdir_safe(s: str) -> pathlib.Path:
    """Safely create a directory.

    If the directory already exists, this function will check if it is empty.
    If not, an exception is raised. Else, you will get back the pathlib.Path object
    pointing to it.
    If parents of the directory does not exists, an exception is raised.

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


def classify_and_convert_dcm(tmp_dirs: List, dest_folder: pathlib.Path) -> None:
    """Rename all .dcm files, classify them on disk (mv) and creates the corresponding .nii file.

    Parameters
    ----------
    tmp_dirs : List
        List of temporary directories where the files are stored.
    dest_folder : pathlib.Path
        The base folder to which put all the processed files.
    """
    dcm_folder = dest_folder / "dcm"
    nii_folder = dest_folder / "nii"
    for dcm_dir in tmp_dirs:
        with dcm_dir as dir_:
            files_obj = [file_obj for file_obj in pathlib.Path(dir_).rglob("*")]
            metadatas = [
                extract_dcm_metadata(str(file_obj), m_reader) for file_obj in files_obj
            ]
            # Write the .nii
            logging.debug(files_obj)
            nii_path = metadata_to_nii_filepath(metadatas[0])
            logging.debug(str(nii_path))
            dcm_to_nii(dir_, nii_folder / nii_path)
            # Move the .dcm
            for m_data in metadatas:
                mv_dcm(m_data, dcm_folder)


def dcm_to_nii(source: pathlib.Path, dest: pathlib.Path) -> None:
    """Convert the given .dcm file to a 3D .nii file.

    Parameters
    ----------
    dest : str
        The file to write.

    Returns
    -------
    None
        Nothing.
    """
    reader = sitk.ImageSeriesReader()
    Ids = reader.GetGDCMSeriesIDs(str(source))
    if len(Ids) != 1:
        raise NotImplementedError(
            "Multiples series in the same folder is not supported"
        )
    Id = Ids[0]
    files = reader.GetGDCMSeriesFileNames(str(source), Id)
    reader.SetFileNames(files)
    image = reader.Execute()
    sitk.WriteImage(image, str(ensure(dest)))
    return None


def threaded_gen(
    pool: ThreadPoolExecutor, func: Callable, ite: Iterable, *args, **kwargs
) -> Generator:
    """Create a multithreaded generator.

    Given a function, an iterable, and a ThreadPoolExecutor, this function
    returns a generator that works in multiple thread.

    Parameters
    ----------
    pool : concurrent.futures.ThreadPoolExecutor
        A ThreadPoolExecutor instance.
    func : Callable
        The function to apply.
    ite : Iterable
        An iterable.

    Yields
    -------
    Any
        Each processed value from the given iterator
    """
    results = []
    for i in ite:
        results.append(pool.submit(func, i, *args, **kwargs))
    for result in as_completed(results):
        logging.debug(result.result().name)
        yield result.result()


def drop_until(predicate: Callable, gen: Iterable) -> Generator:
    """Drop items until the predicate come true.

    The item where the predicate comes True will be dropped.

    Parameters
    ----------
    predicate : Callable
        A function that should return a boolean given each item of the
        Iterable
    gen : Iterable
        The Iterable to filter

    Yields
    -------
        Item from the original Iterator
    """
    for i in gen:
        if predicate(i):
            break
    for i in gen:
        yield i


def ensure(path: pathlib.Path):
    """Create a directory parent if it does not exists.


    Parameters
    ----------
    path : pathlib.Path
        The given path

    Returns
    -------
    pathlib.Path
        The given path, with parents directories created if they don't
        exist.
    """
    # https://stackoverflow.com/questions/53027297/way-for-pathlib-path-rename-to-create-intermediate-directories
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


####################################################################################
##################################### PIPELINE #####################################
####################################################################################

if __name__ == "__main__":
    start = time.perf_counter()

    # setup
    logging.basicConfig(level=logging.DEBUG)
    t_pool = ThreadPoolExecutor(3)  # multiThread
    m_reader = metadata_reader()  # dicom headers' reader
    manifest = pathlib.Path(sys.argv[1])
    destination_folder = mkdir_safe(sys.argv[2])

    # basic checks
    if not all([manifest.exists(), manifest.is_file()]):
        raise ValueError(f"{manifest} does not exist or is not a file")

    # processing pipeline
    open_manifest = manifest.open()
    lines = read_txt(open_manifest)  # manifest file
    lines = (remove_trailing_n(line) for line in lines)
    series_ids = drop_until(lambda x: x == TAKE_AFTER, lines)
    zips = threaded_gen(t_pool, tcia_downloader, series_ids)
    tmp_directories = (unzip_file(zip_) for zip_ in zips)  # full of .dcm
    classify_and_convert_dcm(tmp_directories, destination_folder)

    # Clean up
    t_pool.shutdown()
    open_manifest.close()

    # Performance check
    logging.debug(str(time.perf_counter() - start))
