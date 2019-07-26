from datetime import datetime
import logging
import os
from datetime import datetime
import time
import json

BASE_LOCATION = "/tmp/"
LOG_FORMAT = '%(asctime)s - %(process)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
DATE_FORMAT = '%m/%d/%Y %I:%M:%S %p'

class Logger:
    logger = None

    def __init__(self, log_location):
        if self.logger is None:
            self.logLocation = "{}/{}/{}".format(
                BASE_LOCATION, log_location,
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d'))
            self.filePath = "{}/run_{}.log".format(
                self.logLocation,
                datetime.fromtimestamp(time.time()).strftime('%I:%M:%S'))
            os.makedirs(self.logLocation, exist_ok=True)

            self.logger = logging.getLogger("infra")
            self.logger.setLevel(logging.DEBUG)

            f_handler = logging.FileHandler(self.filePath)
            f_handler.setLevel(logging.DEBUG)

            c_handler = logging.StreamHandler()
            c_handler.setLevel(logging.DEBUG)

            logging.getLogger('parso.cache').disabled = True
            logging.getLogger('parso.cache.pickle').disabled = True
            logging.getLogger('parso.python.diff').disabled = True

            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
            )
            f_handler.setFormatter(formatter)

            self.logger.addHandler(f_handler)
            self.logger.addHandler(c_handler)

    def print_json(self, data):
        self.logger.info(json.dumps(data, indent=2).replace("'", '"'))
