import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyqualitas.checksuite.checksuite import CheckSuite
from pyqualitas.checks.singledfchecks import SingleDataFrameChecks
from pyqualitas.utils.helper import Helper
import os.path


class TestHelper(unittest.TestCase):

    employee_data = [("James", None, "Smith", 36636, "Male", 50000),
                     ("Michael", None, "Rose", 40288, "Male", 60000),
                     ("Robert", None, "Williams", 42114, "Male", 50000),
                     ("Maria", "Anne", "Jones", 39192, "Female", 70000),
                     ("Jen", "Mary", "Brown", 50389, "Female", 90000)]

    employee_schema = StructType([StructField("firstname", StringType(), nullable=False),
                                  StructField("middlename", StringType(), nullable=True),
                                  StructField("lastname", StringType(), nullable=False),
                                  StructField("employee_id", IntegerType(), nullable=False),
                                  StructField("gender", StringType(), nullable=False),
                                  StructField("salary", IntegerType(), nullable=False)])

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.appName("UnitTests").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        print("The spark session has been closed")

    def testCSVReport(self):
        employee = self.spark.createDataFrame(
            data=self.employee_data, schema=self.employee_schema)
        single_df = SingleDataFrameChecks(employee)
        checks = {
            "Test Case 1": {
                "Validate if there are no duplicates in employee table":
                    single_df.check_duplicates()
            },
            "Test Case 2": {
                "Validate if the employee table is not empty":
                    single_df.check_empty()
            }
        }
        check_suite = CheckSuite(checks)
        test_result = check_suite.collect_result()
        helper = Helper(self.spark)
        helper.generate_report_csv(test_result, "TestResult.csv")

    def testHTMLReport(self):
        employee = self.spark.createDataFrame(
            data=self.employee_data, schema=self.employee_schema)
        single_df = SingleDataFrameChecks(employee)
        checks = {
            "Test Case 1": {
                "Validate if there are no duplicates in employee table":
                    single_df.check_duplicates()
            },
            "Test Case 2": {
                "Validate if the employee table is not empty":
                    single_df.check_empty()
            }
        }
        check_suite = CheckSuite(checks)
        test_result = check_suite.collect_result()
        helper = Helper(self.spark)
        helper.generate_html_report(test_result, "TestResult.html")
        self.assertEqual(os.path.exists("TestResult.html"), True)
