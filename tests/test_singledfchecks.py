from pyqualitas.checks.singledfchecks import SingleDataFrameChecks
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class TestSingleDfChecks:

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


    def test_check_duplicates(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_duplicates() == 'Passed'
        assert test_class.check_duplicates(["firstname", "employee_id"]) == 'Passed'
        assert test_class.check_duplicates(["salary"]) == 'Failed'

    def test_check_threshold_count(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_threshold_count(lower_limit=1, upper_limit=5) == 'Passed'
        assert test_class.check_threshold_count(lower_limit=1, upper_limit=4) == 'Failed'

    def test_check_empty(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_empty() == 'Passed'

    def test_check_threshold_sum(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_threshold_sum(lower_limit=50000, upper_limit=320000, sum_column="salary", group_columns=["firstname", "employee_id"]) == 'Passed'
        assert test_class.check_threshold_sum(lower_limit=50000, upper_limit=30000, sum_column="salary", group_columns=["employee_id"]) == 'Failed'

    def test_check_nulls(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_nulls(["firstname", "lastname"]) == 'Passed'
        assert test_class.check_nulls(["middlename"]) == 'Failed'

    def test_check_threshold_nulls(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_threshold_nulls(["middlename"], 0.1, 0.6) == 'Passed'
        assert test_class.check_threshold_nulls(["middlename"], 0.1, 0.4) == 'Failed'

    def test_check_columns(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_columns(["firstname", "middlename", "lastname", "employee_id", "gender", "salary"]) == 'Passed'
        assert test_class.check_columns(["hike_percentage"]) == 'Failed'

    def test_check_patterns(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_pattern("salary", "^[\\d]+$") == 'Passed'

    def test_check_datatype(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_datatype([("firstname", "string"), ("middlename", "string"),
                                          ("lastname", "string"), ("employee_id", "int"),
                                          ("gender", "string"), ("salary", "int")]) == 'Passed'

    def test_check_rank_over_grouping(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_rank_over_grouping(["firstname", "gender"], "salary", [["James"], ["Jen"], ["Maria"], ["Michael"], ["Robert"]]) == 'Passed'

    def test_check_negatives(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_negatives(["salary"]) == 'Passed'

    def test_check_distinct_values(self, spark):
        employee = spark.createDataFrame(data=self.employee_data, schema=self.employee_schema)
        test_class = SingleDataFrameChecks(employee)
        assert test_class.check_distinct_values(column="firstname", values=["James", "Michael", "Robert", "Maria", "Jen"]) == 'Passed'
        assert test_class.check_distinct_values(column="firstname", values=["James", "Michael", "Robert", "Maria"]) == 'Failed'
