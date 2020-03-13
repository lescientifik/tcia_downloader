import logging
import pathlib
import zipfile
from typing import IO, TextIO

log = logging.getLogger(__name__)


class DirectoryNotEmptyError(Exception):
    pass


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


def mv(old_path: pathlib.Path, new_path: pathlib.Path) -> None:
    """Move a file to the new location.

    Parameters
    ----------
    old_path: pathlib.Path
        The file to move
    new_path : pathlib.Path
        The new location to put the file

    Raises
    ------
    FileExistsError
        If the file already exists.

    Note
    ----
    This function is roughly equivalent to mv on Unix. However, if new_path exists,
    it will raises an error.
    """
    if new_path.exists():
        log.error("%s already exists", str(new_path.absolute()))
        raise FileExistsError(str(new_path.absolute()))
    log.info("moving %s to %s", str(old_path), str(new_path))
    old_path.rename(ensure(new_path))


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
        log.debug("Creating %s", str(path))
    except FileExistsError:
        if is_empty(path):
            path.mkdir(exist_ok=True)
            return path
        else:
            raise DirectoryNotEmptyError(path.absolute())
    return path


def unzip_file(file: IO, dest_folder: str) -> None:
    """Unzip the given file

    If you provide an instance of tempfile.NamedTemporaryFile, the file will
    be closed, so it will be completely erased from disk.

    Parameters
    ----------
    file : BinaryIO
        The file to uncompress

    Returns
    -------
    List
        The list of files extracted
    """
    with zipfile.ZipFile(file, "r") as archive:
        archive.extractall(dest_folder)
        log.debug("Extracting files to %s", dest_folder)
        return [dest_folder + "/" + arch for arch in archive.namelist()]


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


def remove_ext(filepath: pathlib.Path) -> pathlib.Path:
    """Remove file extension.

    This function works recursively to remove all extensions found.
    If there is any dot in your filename,
    this function will remove its right side until no dot are left...

    Parameters
    ----------
    filepath : pathlib.Path
        The file

    Returns
    -------
    pathlib.Path
        The new file
    """
    new_path = filepath.with_name(filepath.stem)
    if str(new_path) == str(filepath):
        return new_path
    else:
        return remove_ext(new_path)
