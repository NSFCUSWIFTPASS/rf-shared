import sys
import logging


class Logger:
    def __init__(self, name: str, log_level: str = "DEBUG"):
        # create logger
        self.name = name.lower()
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)

        # prevent duplicate handlers
        if not self.logger.handlers:
            self.logger.propagate = False

            console_handler = logging.StreamHandler(sys.stderr)
            console_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(console_formatter)
            console_handler.setLevel(log_level)
            self.logger.addHandler(console_handler)

    def debug(self, msg: str, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)
