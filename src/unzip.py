# TODO test it
import argparse
from pathlib import Path
from zipfile import ZipFile, is_zipfile

from joblib import Parallel, delayed

parser = argparse.ArgumentParser("unzip all files in a given folder (recursive search)")
parser.add_argument("source", help="the folder with all the zip file")
parser.add_argument("dest", help="the folder where to unzip")
parser.add_argument("--jobs", "-j", help="Number of workers to use", default=4, type=int)


def extract_all_zip(source, dest, n_jobs):
    source = Path(source).expanduser()  # necessary for filetype guess to work
    unzip_root_folder = Path(dest).expanduser()
    unzip_root_folder.mkdir(exist_ok=True)
    print(unzip_root_folder)
    assert source.exists(), f"{source} is not a valid directory"
    print(f"unzipping all files in {source}, using {n_jobs} worker")
    files = list(source.rglob("*"))
    if n_jobs == 1:
        [unzip_file(file, unzip_root_folder) for file in files]
    else:
        Parallel(n_jobs=n_jobs)(delayed(unzip_file)(file, unzip_root_folder) for file in files)


def unzip_file(file, root_folder):
    if is_zipfile(file):
        print(f"found following zip archive: {file.name}")
        print(f"{file}")
        unzip_specific_folder = root_folder / file.name
        unzip_specific_folder.mkdir(exist_ok=True)
        print(f"Decompressing in {unzip_specific_folder}")
        with ZipFile(file) as item:
            item.extractall(unzip_specific_folder)


if __name__ == '__main__':
    args = parser.parse_args()
    print(args)
    extract_all_zip(args.source, args.dest, args.jobs)
