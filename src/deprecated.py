# pylint: skip-file
import tempfile

import pydicom
import SimpleITK as sitk
from src.image_io import metadata_to_dcm_filepath
import pathlib


def get_short_metadata(filename):
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
