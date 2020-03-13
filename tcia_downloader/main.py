import argparse
import csv
import logging
import os
import pathlib
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union

from tcia_downloader.download import tcia_downloader
from tcia_downloader.file_io import mkdir_safe, mv, read_txt, unzip_file
from tcia_downloader.image_io import (
    dcm_to_nii,
    extract_dcm_metadata,
    metadata_reader,
    new_dcmpath_from_metadata,
)
from tcia_downloader.logger import config_basicLogger
from tcia_downloader.utils import drop_until, remove_trailing_n, threaded_gen

# pylint: disable=redefined-outer-name
TAKE_AFTER = "ListOfSeriesToDownload="

log = logging.getLogger(__name__)

# TODO: Rework it
# def classify_dcm(tmp_dirs: List, dest_folder: pathlib.Path) -> None:
#     """Rename all .dcm files, classify them on disk (mv) and creates the corresponding .nii file.

#     Parameters
#     ----------
#     tmp_dirs : List
#         List of temporary directories where the files are stored.
#     dest_folder : pathlib.Path
#         The base folder to which put all the processed files.
#     """
#     for dcm_dir in tmp_dirs:
#         with dcm_dir as dir_:
#             files_obj = [
#                 file_obj
#                 for file_obj in pathlib.Path(dir_).rglob("*")
#                 if file_obj.is_file()
#             ]
#             metadatas = (
#                 extract_dcm_metadata(str(file_obj), m_reader) for file_obj in files_obj
#             )
#             # remove None from the list coming from failed attempt to read the file
#             metadatas = [metadata for metadata in metadatas if metadata]
#             # Move the .dcm
#             for m_data in metadatas:
#                 old_path, new_path = new_dcmpath_from_metadata(m_data, dest_folder)
#                 log.debug("Moving %s to %s", old_path.name, new_path.name)
#                 mv(old_path, new_path)


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


parser = argparse.ArgumentParser(
    description="The CLI to download images from the TCIA website"
)
parser.add_argument("--dest", "-d", help="The folder to download the images")
parser.add_argument("--manifest", "-m", help="The manifest file")


def download():
    args = parser.parse_args()
    dest_folder = args.dest
    manifest = pathlib.Path(args.manifest)
    t_pool = ThreadPoolExecutor(3)  # multiThread
    destination_folder = mkdir_safe(dest_folder)
    log_file = destination_folder / "download"
    config_basicLogger(str(log_file))
    log.debug(
        "destination folder: %s | manifest: %s", str(destination_folder), str(manifest)
    )
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
    series_folders = []
    dcm_list = []
    for archive, serie_id in zips:
        log.debug("series_id is %s", serie_id)
        series_folder = pathlib.Path(str(destination_folder / serie_id))
        series_folder.mkdir(exist_ok=True)
        if series_folder in series_folders:
            log.warning("%s already exists: pray for not collapsing names!")
            series_folders.append(series_folder)
        dcms = unzip_file(archive, str(series_folder))
        collapsing_dcms = [duplicate for duplicate in dcms if duplicate in dcm_list]
        for collapsing_dcm in collapsing_dcms:
            log.warning("%s already exists, will be overwritten", collapsing_dcm)
        dcm_list.extend(dcms)
    dcms_count = Counter(dcm_list)
    duplicates = [
        (dcm_count, dcms_count[dcm_count])
        for dcm_count in dcms_count
        if dcms_count[dcm_count] > 1
    ]
    if duplicates:
        with open(str(destination_folder / "duplicates.csv"), "wb") as csvfile:
            dup_writer = csv.writer(csvfile, delimiter=";")
            dup_writer.writerows(duplicates)
    else:
        with open(str(destination_folder / "noduplicates.csv"), "wb"):
            pass


####################################################################################
##################################### PIPELINE #####################################
####################################################################################

# if __name__ == "__main__":
#     start = time.perf_counter()

#     # some hacking of python path
#     # to work without having to "pip install -e ."
#     package_dir = pathlib.Path("../../")
#     package_dir = package_dir.absolute()
#     sys.path.append(str(package_dir))

#     # setup
#     t_pool = ThreadPoolExecutor(3)  # multiThread
#     m_reader = metadata_reader()  # dicom headers' reader
#     manifest = pathlib.Path(sys.argv[1])
#     destination_folder = mkdir_safe(sys.argv[2])
#     log_file = destination_folder / "log.txt"
#     logging.basicConfig(level=logging.DEBUG, filename=str(log_file))
#     dcm_folder = destination_folder / "dcm"
#     # basic checks
#     if not all([manifest.exists(), manifest.is_file()]):
#         raise ValueError(f"{manifest} does not exist or is not a file")

#     # processing pipeline
#     open_manifest = manifest.open()
#     lines = read_txt(open_manifest)  # manifest file
#     lines = (remove_trailing_n(line) for line in lines)
#     series_ids = drop_until(lambda x: x == TAKE_AFTER, lines)
#     zips = threaded_gen(t_pool, tcia_downloader, series_ids)
#     tmp_directories = (unzip_file(zip_) for zip_ in zips)  # full of .dcm
#     classify_dcm(tmp_directories, dcm_folder)
#     batch_convert_dcm_to_nii(destination_folder)

#     # Clean up
#     t_pool.shutdown()
#     open_manifest.close()

#     # Performance check
#     log.debug(str(time.perf_counter() - start))
