# TODO test it
import argparse
from pathlib import Path
from zipfile import ZipFile, is_zipfile


def extract_all_zip():
    parser = argparse.ArgumentParser("unzip a given folder")
    parser.add_argument("folder", "the folder with all the zip file")
    args = parser.parse_args()
    folder = Path(args.folder).expanduser()  # necessary for filetype guess to work
    unzip_root_folder = folder / "unzip"
    unzip_root_folder.mkdir()
    assert folder.exists(), f"{folder} is not a valid directory"
    files = folder.rglob("*")
    for file in files:
        if is_zipfile(file):
            print(f"found following zip archive: {file.name}")
            print(f"Decompressing in {str(unzip_root_folder / str(file))}/")
            unzip_specific_folder = unzip_root_folder / str(file)
            unzip_root_folder.mkdir(exist_ok=True)
            with ZipFile(file) as item:
                item.extractall(unzip_specific_folder)


if __name__ == '__main__':
    extract_all_zip()
