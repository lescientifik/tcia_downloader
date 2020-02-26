import logging
import pathlib
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union
import os

from tcia_downloader.download import tcia_downloader
from tcia_downloader.file_io import mkdir_safe, read_txt, unzip_file, mv
from tcia_downloader.image_io import (
    dcm_to_nii,
    extract_dcm_metadata,
    metadata_reader,
    new_dcmpath_from_metadata,
)
from tcia_downloader.utils import drop_until, remove_trailing_n, threaded_gen

TAKE_AFTER = "ListOfSeriesToDownload="

log = logging.getLogger(__name__)


def classify_dcm(tmp_dirs: List, dest_folder: pathlib.Path) -> None:
    """Rename all .dcm files, classify them on disk (mv) and creates the corresponding .nii file.

    Parameters
    ----------
    tmp_dirs : List
        List of temporary directories where the files are stored.
    dest_folder : pathlib.Path
        The base folder to which put all the processed files.
    """
    for dcm_dir in tmp_dirs:
        with dcm_dir as dir_:
            files_obj = [
                file_obj
                for file_obj in pathlib.Path(dir_).rglob("*")
                if file_obj.is_file()
            ]
            metadatas = (
                extract_dcm_metadata(str(file_obj), m_reader) for file_obj in files_obj
            )
            # remove None from the list coming from failed attempt to read the file
            metadatas = [metadata for metadata in metadatas if metadata]
            # Move the .dcm
            for m_data in metadatas:
                old_path, new_path = new_dcmpath_from_metadata(m_data, dest_folder)
                log.debug("Moving %s to %s", old_path.name, new_path.name)
                mv(old_path, new_path)


def batch_convert_dcm_to_nii(base_folder: Union[str, pathlib.Path]) -> None:
    """Convert all .dcm files contained in a given directory (with recursion) to .nii.gz

    Parameters
    ----------
    base_folder : Folder that contain
        The folder to process.

    Raises
    ------
    FileExistsError
        The destination file already exists
    NotImplementedError
        If the dicom file path is not as supposed by this function
    ValueError
        The child folder containing the .dcm also contains other type of files.
        This will be most probably raised if you apply this function without using
        `classify_dcm` first

    """
    for root, dirs, files in os.walk(str(base_folder)):
        if files:
            source = pathlib.Path(root)
            all_dcm = all(
                pathlib.Path(f"{root}/{file}").match("*.dcm") for file in files
            )
            if all_dcm:
                parts = list(source.parts)
                ## ex: a/dcm/c/d/1.dcm => a/nii/c/d.nii.gz
                ## ex: a/b/dcm/c/1.dcm => a/b/nii/c.nii.gz
                log.debug(parts)
                if "dcm" not in parts:
                    raise NotImplementedError(
                        "This function should not be called outside the main function"
                    )
                # replace "dcm" by "nii" in the list
                for i, part in enumerate(parts):
                    if part == "dcm":
                        parts[i] = "nii"
                log.debug(parts)
                dest = "/".join(parts) + ".nii.gz"
                dest = pathlib.Path(dest)
                log.debug(str(dest))
                ##
                dcm_to_nii(source, dest)
                log.info("Created %s", str(dest))
            else:
                raise FileExistsError(
                    f"This function should be called on a folder created by classify_dcm function"
                    f" or at least a folder containing only .dcm files"
                )


####################################################################################
##################################### PIPELINE #####################################
####################################################################################

if __name__ == "__main__":
    start = time.perf_counter()

    # some hacking of python path
    # to work without having to "pip install -e ."
    package_dir = pathlib.Path("../../")
    package_dir = package_dir.absolute()
    sys.path.append(str(package_dir))

    # setup
    t_pool = ThreadPoolExecutor(3)  # multiThread
    m_reader = metadata_reader()  # dicom headers' reader
    manifest = pathlib.Path(sys.argv[1])
    destination_folder = mkdir_safe(sys.argv[2])
    log_file = destination_folder / "log.txt"
    logging.basicConfig(level=logging.DEBUG, filename=str(log_file))
    dcm_folder = destination_folder / "dcm"
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
    classify_dcm(tmp_directories, dcm_folder)
    batch_convert_dcm_to_nii(destination_folder)

    # Clean up
    t_pool.shutdown()
    open_manifest.close()

    # Performance check
    log.debug(str(time.perf_counter() - start))
