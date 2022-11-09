from time import sleep


class CheckSuite:

    def __init__(self, checks, retries=10, sleep_time=60):
        self.checks = checks
        self.retries = retries
        self.sleep_time = sleep_time
        self.results_list = []

    def collect_result(self):
        """
        Summary: This function collects the result of the test checks defined by the user

        Parameters: Number of retries and the wait time for the tests to complete the execution. Default value is 10, 60

        Output: Returns a List of Tuples with Test Case Name, Test Description and Test Result
        """
        for test_name, values in self.checks.items():
            for test_description, result in values.items():
                retry_count = 0
                while retry_count <= self.retries:
                    if result not in ["Passed", "Failed"]:
                        sleep(self.sleep_time)
                        retry_count += 1
                    else:
                        self.results_list.append([test_name, test_description, result])
                        break
        return self.results_list
