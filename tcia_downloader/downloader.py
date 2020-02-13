import logging
import pathlib
from functools import partial
from typing import Dict

import requests

# pylint: disable=logging-format-interpolation


def url_download(url: str, path: str, params: Dict):

    r = requests.get(url, params=params, stream=True)

    with open(path, "wb") as f:

        for ch in r:

            f.write(ch)
    return


tcia_images_download = partial(
    url_download,
    url="https://services.cancerimagingarchive.net/services/v3/TCIA/query/getImage",
)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("trying to dl one image from TCIA")
    save_path = pathlib.Path("./test.zip")
    logging.info(f"Savepath is {save_path.absolute()}")
    tcia_images_download(
        path=str(save_path),
        params={
            "SeriesInstanceUID": "1.3.6.1.4.1.14519.5.2.1.3098.5025.242083141114562987765795908595"
        },
    )
    logging.info(f"Done")
