import logging


class CustomLogger:

    """
    Summary: This class can be used to instantiate the logging functionality  

    Parameters: Filename where the log has to be saved & Log level ->  NOTSET = 0; DEBUG = 10; INFO = 20; WARNING = 30; ERROR = 40; CRITICAL = 50

    Output: Returns an instance of the logger
    
    """

    def __init__(self, file_name, log_level):
        self.logger = logging.getLogger(__name__)
        self.formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        self.file_handler = logging.FileHandler(file_name)
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)
        self.log_level = log_level

    def instantiate(self):
        safe_list = [0, 10, 20, 30, 40, 50]
        if self.log_level in safe_list:
            self.logger.setLevel(self.log_level)
        else:
            self.logger.setLevel(10)
        return self.logger

