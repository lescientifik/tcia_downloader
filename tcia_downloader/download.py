import tempfile
from typing import IO

import requests

# source
TCIA_ENDPOINT = (
    "https://services.cancerimagingarchive.net/services/v3/TCIA/query/getImage"
)


def tcia_downloader(seriesID: str) -> IO:
    """Download a file using requests.

    TODO
    See https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
    Keeping the same session object for the same endpoint could lead to improved
    performance.
    See https://stackoverflow.com/questions/24873927/python-requests-module-and-connection-reuse
    for more information.

    Parameters
    ----------
    seriesID : str
        The parsed string from manifest.tcia file, giving the serie to download.

    Returns
    -------
    IO (tempfile.NamedTemporaryFile)
        An open temporary file containing the downloaded series (.zip)
    """
    tmp_file = tempfile.NamedTemporaryFile()
    with requests.get(
        TCIA_ENDPOINT, params={"SeriesInstanceUID": seriesID}, stream=True
    ) as r:
        r.raise_for_status()
        # f = open(tmp_file, "wb")
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:  # filter out keep-alive new chunks
                tmp_file.write(chunk)
        return tmp_file
