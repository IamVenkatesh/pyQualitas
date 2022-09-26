import imp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
import pandas as pd


class SingleDataFrameChecks:

    def __init__(self, spark: SparkSession, dataframe: DataFrame):

        self.spark = spark
        self.dataframe = dataframe

    def check_duplicates(self, columns):

        """
        Summary: This function is used to check if there are any duplicate values in the user-defined columns. 
        Kindly note that the columns mentioned in the list will be used independently and not in a combined fashion.

        Parameters: List of columns for which the duplicate values have to checked.

        Output: Returns the status of the test i.e. Passed or Failed

        """
        columns_with_duplicates = 0
        status ='Running'

        for column in columns:
            distinct_count = self.dataframe.select(column).distinct().count()
            total_count = self.dataframe.select(column).count()

            if distinct_count == total_count:
                print("The column {0} has no duplicate values".format(column))
            else:
                duplicates = self.dataframe.groupBy(cols=column).agg(count('*').alias('count')).filter(col('count') > 1).toPandas()
                duplicate_values = set(duplicates[column])
                print("The column {0} has duplicate values. The following are the list of duplicates: {1}".format(column, duplicate_values))
                columns_with_duplicates += 1

        if columns_with_duplicates > 0:
            status = 'Failed'
        else: 
            status = 'Passed'
        
        return status
    
    def check_count(self, lower_limit, upper_limit):

        """
        Summary: This function is used to check if the total record count in the table is between the limits set by the user.

        Parameters: Lower Limit & Upper Limit for the records counts

        Output: Returns the status of the test i.e. Passed or Failed
        
        """

        status = 'Running'
        total_count = self.dataframe.count()

        if lower_limit <= total_count <= upper_limit:
            print("The count is between the defined lower and upper limits. The count for the table is: {0}".format(total_count))
            status = 'Passed'
        else:
            print("The count is not between the defined lower and upper limits. The count for the table is: {0}".format(total_count))
            status = 'Failed'
        
        return status
    
    def check_empty(self):

        """
        Summary: This function is used to check if the table has records or not.

        Output: Returns the status of the test i.e. Passed or Failed
        
        """

        status = 'Running'
        total_count = self.dataframe.count()

        if total_count > 0:
            print("The table is not empty. The total record count in the table is: {0}".format(total_count))
            status = 'Passed'
        else:
            print("The table is empty")
            status = 'Failed'

        return status


