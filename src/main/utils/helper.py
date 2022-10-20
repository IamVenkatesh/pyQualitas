from pyspark.sql import SparkSession
import pandas as pd
import json
from flask import render_template



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
        Summary: This function is a helper to create the html report to display the results generated in checksuite.
        The output returned from this function can be written as a HTML file.

        Parameters: Dataframe containing test results

        Output: Returns the rendered HTML template as string
        """
        results = pd.DataFrame(test_result).reset_index()
        data = json.loads(results.to_json(orient='records'))
        context = {'d': data}
        html_output = render_template('./template/TestResultTemplate.html', d=context)
        return html_output
        

        