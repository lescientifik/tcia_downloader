import argparse
import json
import pathlib
from concurrent.futures import ProcessPoolExecutor, as_completed

import requests

from src.file_io import mkdir_safe, read_txt
from src.utils import drop_until, remove_trailing_n

TAKE_AFTER = "ListOfSeriesToDownload="
TCIA_ENDPOINT = (
    "https://services.cancerimagingarchive.net/services/v3/TCIA/query/getImage"
)


def tcia_dl(serie_id: str, dest_file: pathlib.Path) -> pathlib.Path:
    """Download a file using requests.
    """
    with requests.get(
            TCIA_ENDPOINT, params={"SeriesInstanceUID": serie_id}, stream=True
    ) as r:
        r.raise_for_status()
        metadata = r.headers.get("metadata")
        metadata = json.loads(metadata)
        filetype = metadata.get("Result").get("Type")[0]
        if filetype != "ZIP":
            raise ValueError(
                "Supplied seriesID is not valid. No .zip file here", serie_id
            )
        with dest_file.open("wb") as file:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)
        print(f"Series {serie_id} downloaded at {dest_file}")
        assert dest_file.exists()
        return dest_file


parser = argparse.ArgumentParser(
    description="The CLI to download images from the TCIA website"
)
parser.add_argument("--dest", "-d", help="The folder to download the images")
parser.add_argument("--manifest", "-m", help="The manifest file")


def download():
    args = parser.parse_args()
    dest_folder = args.dest
    manifest = pathlib.Path(args.manifest)
    destination_folder = mkdir_safe(dest_folder)
    print(f"destination folder: {destination_folder} | manifest: {manifest}")
    # basic checks
    if not all([manifest.exists(), manifest.is_file()]):
        raise ValueError(f"{manifest} does not exist or is not a file")

    # processing pipeline
    open_manifest = manifest.open()
    lines = read_txt(open_manifest)  # manifest file
    lines = (remove_trailing_n(line) for line in lines)
    series_id = drop_until(lambda x: x == TAKE_AFTER, lines)
    with ProcessPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(tcia_dl, serie_id, destination_folder / serie_id): serie_id
                   for
                   serie_id in series_id}
        for future in as_completed(futures):
            future.result()
            # file = future.result()
            # print(f"{file} downloaded !")
            # assert file.exists(), "File has not been downloaded properly!"


if __name__ == '__main__':
    download()
    #     for serie_id in series_id:
    #         filename = serie_id + ".zip"
    #         future =
    #
    # zips = threaded_gen(t_pool, tcia_downloader, series_ids)
    # series_folders = []
    # dcm_list = []
    # for archive, serie_id in zips:
    #     print.debug("series_id is %s", serie_id)
    #     series_folder = pathlib.Path(str(destination_folder / serie_id))
    #     series_folder.mkdir(exist_ok=True)
    #     if series_folder in series_folders:
    #         log.warning("%s already exists: pray for not collapsing names!")
    #         series_folders.append(series_folder)
    #     dcms = unzip_file(archive, str(series_folder))
    #     collapsing_dcms = [duplicate for duplicate in dcms if duplicate in dcm_list]
    #     for collapsing_dcm in collapsing_dcms:
    #         log.warning("%s already exists, will be overwritten", collapsing_dcm)
    #     dcm_list.extend(dcms)
    # dcms_count = Counter(dcm_list)
    # duplicates = [
    #     (dcm_count, dcms_count[dcm_count])
    #     for dcm_count in dcms_count
    #     if dcms_count[dcm_count] > 1
    # ]
    # if duplicates:
    #     with open(str(destination_folder / "duplicates.csv"), "wb") as csvfile:
    #         dup_writer = csv.writer(csvfile, delimiter=";")
    #         dup_writer.writerows(duplicates)
    # else:
    #     with open(str(destination_folder / "noduplicates.csv"), "wb"):
    #         pass

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
#     zips = threaded_gen(t_pool, src, series_ids)
#     tmp_directories = (unzip_file(zip_) for zip_ in zips)  # full of .dcm
#     classify_dcm(tmp_directories, dcm_folder)
#     batch_convert_dcm_to_nii(destination_folder)

#     # Clean up
#     t_pool.shutdown()
#     open_manifest.close()

#     # Performance check
#     log.debug(str(time.perf_counter() - start))
