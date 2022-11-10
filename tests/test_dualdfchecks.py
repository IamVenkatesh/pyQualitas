import unittest
from pyqualitas.checks.dualdfchecks import DualDataFrameChecks
from pyqualitas.utils.helper import Helper
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class TestDualDfChecks(unittest.TestCase):

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

    employee_dimension = [("James", None, "Smith", 36636, 1),
                          ("Michael", None, "Rose", 40288, 2),
                          ("Robert", None, "Williams", 42114, 1),
                          ("Maria", "Anne", "Jones", 39192, 2),
                          ("Jen", "Mary", "Brown", 50389, 3)]

    employee_dimension_schema = StructType([StructField("firstname", StringType(), nullable=False),
                                            StructField("middlename", StringType(), nullable=True),
                                            StructField("lastname", StringType(), nullable=False),
                                            StructField("employee_id", IntegerType(), nullable=False),
                                            StructField("department_id", IntegerType(), nullable=False)])

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.appName("UnitTests").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        print("The spark session has been closed")

    def test_check_columns(self):
        helper = Helper(self.spark)
        df1 = helper.create_dataframe(self.employee_data, self.employee_schema)
        df2 = helper.create_dataframe(self.employee_data, self.employee_schema)
        dual_check = DualDataFrameChecks(df1, df2)
        self.assertEqual(dual_check.check_columns(), 'Passed')

    def test_check_datatype(self):
        helper = Helper(self.spark)
        df1 = helper.create_dataframe(self.employee_data, self.employee_schema)
        df2 = helper.create_dataframe(self.employee_data, self.employee_schema)
        dual_check = DualDataFrameChecks(df1, df2)
        self.assertEqual(dual_check.check_datatype(), 'Passed')

    def test_check_distinct_values(self):
        helper = Helper(self.spark)
        df1 = helper.create_dataframe(self.employee_data, self.employee_schema)
        df2 = helper.create_dataframe(self.employee_dimension, self.employee_dimension_schema)
        dual_check = DualDataFrameChecks(df1, df2)
        self.assertEqual(dual_check.check_distinct_values("employee_id", "employee_id"), 'Passed')

    def test_check_count(self):
        helper = Helper(self.spark)
        df1 = helper.create_dataframe(self.employee_data, self.employee_schema)
        df2 = helper.create_dataframe(self.employee_data, self.employee_schema)
        dual_check = DualDataFrameChecks(df1, df2)
        self.assertEqual(dual_check.check_count(), 'Passed')

    def test_check_compare_data(self):
        helper = Helper(self.spark)
        df1 = helper.create_dataframe(self.employee_data, self.employee_schema)
        df2 = helper.create_dataframe(self.employee_data, self.employee_schema)
        df3 = df2.limit(4)
        dual_check = DualDataFrameChecks(df1, df2)
        self.assertEqual(dual_check.check_compare_data(["firstname", "lastname"]), 'Passed')
        dual_check_failure = DualDataFrameChecks(df1, df3)
        self.assertEqual(dual_check_failure.check_compare_data(["firstname", "lastname"]), 'Failed')


if __name__ == '__main__':
    unittest.main()
