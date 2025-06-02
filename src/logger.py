import logging
import os
from datetime import datetime
from utils import LOGS_DIR

def get_logger(log_filename):
    """
    Returns a logger configured with the specified log file.
    The logger is configured only once per file.
    """
    log_file_path = os.path.join(LOGS_DIR, log_filename)

    logger = logging.getLogger(log_filename)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_file_path, mode = 'w')
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger