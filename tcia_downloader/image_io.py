import logging
import pathlib
from typing import Dict, Tuple

import SimpleITK as sitk

from tcia_downloader.file_io import ensure
from tcia_downloader.utils import get_valid_filepath

log = logging.getLogger(__name__)


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
    try:
        reader.ReadImageInformation()
        metadata = {
            key: reader.GetMetaData(key)
            for key in data_elements_machine
            if reader.HasMetaDataKey(key)
        }
        metadata.update(file=file_object)
        metadata = {**default_elements_machine, **metadata}
        return metadata
    except RuntimeError as read_error:  # file is not a valid dcm
        log.error(read_error)
        return None


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


def new_dcmpath_from_metadata(
    metadata: Dict, base_folder: pathlib.Path
) -> Tuple[pathlib.Path, pathlib.Path]:
    old_path = metadata["file"]
    new_path = base_folder / metadata_to_dcm_filepath(metadata)
    return (old_path, new_path)


def dcm_to_nii(source: pathlib.Path, dest: pathlib.Path) -> None:
    """Convert the given .dcm files from source folder to a 3D .nii file.

    Parameters
    ----------
    source: pathlib.Path
        The source directory containing all .dcm files
    dest : pathlib.Path
        The file to write.

    Returns
    -------
    None
        Nothing.
    """
    if pathlib.Path(dest).exists():
        raise FileExistsError(f"File already exists: {dest}")
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
