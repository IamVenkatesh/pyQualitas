from time import sleep


class CheckSuite:

    def __init__(self, checks, sleep_time=60):
        self.checks = checks
        self.sleep_time = sleep_time
        self.results_list = []

    def collect_result(self):
        """
        Summary: This function collects the result of the test checks defined by the user

        Parameters: Number of retries and the wait time for the tests to complete the execution. Default value is 10, 60

        Output: Returns a List with Test Case Name, Test Description and Test Result
        """
        total_test = len(self.checks)
        while total_test > 0:
            for test_name, values in self.checks.items():
                for test_description, result in values.items():
                    if result in ["Passed", "Failed"]:
                        total_test = total_test - 1
                        self.results_list.append([test_name, test_description, result])
                    else:
                        continue
            sleep(self.sleep_time)
                        
        return self.results_list
