import sys
import logging


def setup_logging(log_level: str = "INFO", root_logger_name: str | None = None):
    """
    Configures the root logger for the application.
    """
    # Get the target logger. If no name is provided, it's the absolute root logger.
    log = logging.getLogger(root_logger_name)

    # Set the level on this logger.
    log.setLevel(log_level)

    # If a handler is already configured, we assume setup has been done elsewhere.
    if log.hasHandlers():
        log.debug("Logger already has handlers; skipping setup.")
        return

    # Create a handler to write to stderr
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(log_level)

    # Create a formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    # Add the handler to the logger
    log.addHandler(handler)

    log.info(f"Logging configured for '{log.name or 'root'}' at level {log_level}.")
