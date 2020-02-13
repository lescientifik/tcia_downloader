import logging
import pathlib

import requests

# pylint: disable=logging-format-interpolation

logger = logging.getLogger("TCIA DOWNLOAD")

API_ENDPOINT = (
    "https://services.cancerimagingarchive.net/services/v3/TCIA/query/getImage"
)
QUERY_PARAM_NAME = "SeriesInstanceUID"


def tcia_images_download(path: str, instanceID: str):

    logger.info(f"starting download of {instanceID}")
    save_path = pathlib.Path(path)
    logger.info(f"Savepath is {save_path.absolute()}")

    response = requests.get(
        API_ENDPOINT, params={QUERY_PARAM_NAME: instanceID}, stream=True
    )

    with save_path.open(mode="wb") as f:
        for chunk in response:
            f.write(chunk)

    return


if __name__ == "__main__":
    logger.info("trying to dl one image from TCIA")
    test_path = pathlib.Path("./test.zip")
    logger.info(f"Testpath is {test_path.absolute()}")
    tcia_images_download(
        path=str(test_path),
        instanceID="1.3.6.1.4.1.14519.5.2.1.3098.5025.242083141114562987765795908595",
    )
