import logging
from pyqualitas.utils.logger import CustomLogger


class ArbitaryChecks:

    def __init__(self, log_file_location='arbitarychecks.log'):
        if not logging.getLogger(__name__).hasHandlers():
            self.logger_instance = CustomLogger(log_file_location, 10, __name__)
            self.logger = self.logger_instance.instantiate()
        else:
            self.logger = logging.getLogger(__name__)
        self.check_number = 1
    
    def arbitary_check(self, check, expected_result):
        if check == expected_result:
            self.logger.info("The arbitary check - {0} has passed".format(self.check_number))
            status = 'Passed'
        else:
            self.logger.warning("The arbitary check - {0} has failed".format(self.check_number))
            status = 'Failed'
        self.check_number += 1
        return status
