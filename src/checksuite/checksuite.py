from time import sleep


class CheckSuite:

    def __init__(self, checks, retries):
        self.checks = checks
        self.retries = retries
        self.results = []

    def collect_result(self, number_of_retries=10, sleep_time=60):
        """
        Summary: This function collects the result of the test checks defined by the user

        Parameters: Number of retries and the wait time for the tests to complete the execution. Default value is 10, 60

        Output: Returns a List of Tuples with Test Case Name, Test Description and Test Result
        """
        for test_name, values in self.checks.items():
            for test_description, result in values.items():
                retry_count = 0
                while retry_count <= number_of_retries:
                    if result not in ["Passed", "Failed"]:
                        sleep(sleep_time)
                        retry_count += 1
                    else:
                        self.results.append((test_name, test_description, result))
                        break
        return self.results
