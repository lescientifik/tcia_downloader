import logging




def config_basicLogger(logfile: str) -> None:
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG, handlers=[logging.FileHandler(logfile + ".log", mode="w"), stream_handler],
    )
