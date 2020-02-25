import logging
import pathlib
import tempfile
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
    log.debug("Extracting files from %s", tmp_dir.name)
    return tmp_dir


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
