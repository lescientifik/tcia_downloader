import json
import logging
import tempfile
from typing import IO, Tuple

import requests

log = logging.getLogger(__name__)

# source
TCIA_ENDPOINT = (
    "https://services.cancerimagingarchive.net/services/v3/TCIA/query/getImage"
)


def tcia_downloader(seriesID: str) -> Tuple[IO, str]:
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
        metadata = r.headers.get("metadata")
        metadata = json.loads(metadata)
        filetype = metadata.get("Result").get("Type")[0]
        if filetype != "ZIP":
            raise ValueError(
                "Supplied seriesID is not valid. No .zip file here", seriesID
            )
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:  # filter out keep-alive new chunks
                tmp_file.write(chunk)
        log.info("Series %s downloaded at %s", seriesID, tmp_file.name)
        return (tmp_file, seriesID)
