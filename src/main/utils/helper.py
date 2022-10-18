from pyspark.sql import SparkSession
import pandas as pd
import json



class Helper:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def create_hive_table_df(self, database_name, table_name):
        """
        Summary: This function is a helper to read the hive database table to spark dataframe

        Parameters: Database Name & Table Name

        Output: Returns a spark dataframe
        """
        return self.spark.table("{0}.{1}".format(database_name, table_name))

    def create_hdfs_files_df(self, format_type, hdfs_location, infer_schema=True):
        """
        Summary: This function is a helper to read the hdfs files to spark dataframe

        Parameters: HDFS File Format, HDFS File Location, True or False for inferSchema. By Default the values is
        set to true.

        Output: Returns a spark dataframe
        """
        return self.spark.read.format(format_type).option("header", True) \
            .option("inferSchema", infer_schema).load(hdfs_location)

    def create_dataframe(self, dataframe, schema):
        """
        Summary: This function is a helper to create a spark dataframe

        Parameters: Data on which the dataframe has to be created, schema of the data

        Output: Returns a spark dataframe
        """
        return self.spark.createDataFrame(data=dataframe, schema=schema)

    def create_html_report(self, test_result):
        """
        Summary: This function is a helper to create the html report to display the results generated in checksuite

        Parameters: Dataframe containing test results, Location with the filename to which the results have to be saved

        Output: Writes the html file to the user defined location
        """
        results = pd.DataFrame(test_result).reset_index()
        data = []
        data = json.loads(results.to_json(orient ='records'))
        context = {'d': data}
        

        