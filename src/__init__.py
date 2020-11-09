import logging
import pathlib

app_name = str(pathlib.Path("./").resolve().parts[-1])
log = logging.getLogger(app_name).addHandler(logging.NullHandler())
