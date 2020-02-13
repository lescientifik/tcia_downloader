import pathlib
import logging
import argparse

logging.getLogger("TCIA_DOWNLOAD")
parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", help="The path to the TCIA manifest file to parse")


def get_series_to_dl(manifest_path: str):
    file = pathlib.Path(manifest_path)
    if not file.exists():
        raise ValueError("Manifest_path does not exist")
    with file.open("r") as f:
        start_line = "ListOfSeriesToDownload="
        start = False
        while True:
            line = f.readline()
            if not line:
                break
            line = line.rstrip("\n")
            if start_line == line:
                start = True
            elif start:
                instance_id = line
                logging.debug(instance_id)
                yield instance_id
        logging.info("Last image to download read from disk")


if __name__ == "__main__":
    args = parser.parse_args()
    images = list(get_series_to_dl(args.file))
    logging.info(images[0])
