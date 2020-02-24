import re
from concurrent.futures import as_completed, ThreadPoolExecutor
from typing import Callable, Iterable, Generator
import logging


log = logging.getLogger(__name__)


def remove_trailing_n(line: str) -> str:
    return line.rstrip("\n")


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
        log.debug(result.result().name)
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
