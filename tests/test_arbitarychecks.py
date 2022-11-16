import unittest
from pyqualitas.checks.arbitarychecks import ArbitaryChecks
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class TestArbitartChecks(unittest.TestCase):

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

    def testarbitarycheck(self):
        employee = self.spark.createDataFrame(
            data=self.employee_data, schema=self.employee_schema)
        arbitary_test = ArbitaryChecks()
        test_1 = arbitary_test.arbitary_check(employee.columns, ["firstname", "middlename", "lastname", "employee_id", "gender", "salary"])
        test_2 = arbitary_test.arbitary_check(employee.dtypes, [("firstname", "string"), ("middlename", "string"),
                                                                ("lastname", "string"), ("employee_id", "int"),
                                                                ("gender", "string"), ("salary", "int")])
        self.assertEqual(test_1, 'Passed')
        self.assertEqual(test_2, 'Passed')