class CheckSuite:

    def __init__(self, checks):
        self.checks = checks
        self.results = []

    def collect_result(self):
        for test_name, values in self.checks.items():
            for test_description, result in values.items():
                self.results.append((test_name, test_description, result))
        return self.results
