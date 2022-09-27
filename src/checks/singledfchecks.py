from pyspark.sql import functions
from pyspark.sql.functions import sum


class SingleDataFrameChecks:

    def __init__(self, dataframe):

        self.dataframe = dataframe

    def check_duplicates(self, columns):

        """
        Summary: This function is used to check if there are any duplicate values in the user-defined columns. 
        Kindly note that the columns mentioned in the list will be used independently and not in a combined fashion.

        Parameters: List of columns for which the duplicate values have to checked.

        Output: Returns the status of the test i.e. Passed or Failed

        """
        columns_with_duplicates = 0

        for column in columns:
            distinct_count = self.dataframe.select(column).distinct().count()
            total_count = self.dataframe.select(column).count()

            if distinct_count == total_count:
                print("The column {0} has no duplicate values".format(column))
            else:
                duplicates = self.dataframe.groupBy(column).agg(functions.count('*').alias('count')).filter(functions.col('count') > 1)
                duplicate_values = set(duplicates.select(column).rdd.map(lambda x : x[0]).collect())
                print("The column {0} has duplicate values. The following are the list of duplicates: {1}".format(column, duplicate_values))
                columns_with_duplicates += 1

        if columns_with_duplicates > 0:
            status = 'Failed'
        else: 
            status = 'Passed'
        
        return status
    
    def check_threshold_count(self, lower_limit, upper_limit):

        """
        Summary: This function is used to check if the total record count in the table is between the limits set by the user.

        Parameters: Lower Limit & Upper Limit for the records counts

        Output: Returns the status of the test i.e. Passed or Failed
        
        """

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

        total_count = self.dataframe.count()

        if total_count > 0:
            print("The table is not empty. The total record count in the table is: {0}".format(total_count))
            status = 'Passed'
        else:
            print("The table is empty")
            status = 'Failed'

        return status

    def check_threshold_sum(self, lower_limit, upper_limit, sum_column ,group_columns):

        """
        Summary: This function is used to check if the sum of a column in the table is between the limits set by the user.

        Parameters: Lower Limit & Upper Limit for validating the sum, column for generating the sum, optional list of columns for group by. 
        If not mentioned, the group by will consider all the columns

        Output: Returns the status of the test i.e. Passed or Failed
        
        """
        threshold_sum_count = 0
        total_sum = self.dataframe.groupBy(group_columns).agg(sum(sum_column).alias('total'))
        total_sum_value = total_sum.select('total').rdd.map(lambda x : x[0]).collect()

        for value in total_sum_value:
            if lower_limit <= value <= upper_limit:
                continue
            else:
                threshold_sum_count += 1

        if threshold_sum_count > 0:
            print("There are {0} grouping instances where the sum value is not within the define threshold".format(threshold_sum_count))
            status = 'Failed'
        else:
            status = 'Passed'
        
        return status
    
    def check_nulls(self, columns):

        """
        Summary: This function is used to check if there are null values in the user specified list of columns

        Parameters: Column names as a list on which the check has to be performed

        Output: Returns the status of the test i.e. Passed or Failed
        
        """
        
        columns_with_nulls = 0

        for column in columns:
            count_without_nulls = self.dataframe.na.drop(subset=column).count()
            total_count = self.dataframe.count()

            if count_without_nulls == total_count:
                print("The column: {0} has no null values".format(column))
            else:
                null_count = total_count - count_without_nulls
                print("The column: {0} contains {1} null values".format(column, null_count))
                columns_with_nulls += 1

        if columns_with_nulls > 0:
            status = 'Failed'
        else:
            status = 'Passed'
    
        return status
        
    def check_threshold_nulls(self, columns, lower_limit, upper_limit):

        """
        Summary: This function is used to check if the null values in the user specified columns list are within the defined percentage limits.
        The percentage limits has to be defined as >= 0.01 for lower limit and <= 1.0 for upper limit.  

        Parameters: Column names as a list on which the check has to be performed, lower & upper limit for the count of null values

        Output: Returns the status of the test i.e. Passed or Failed
        
        """

        columns_with_nulls = 0

        for column in columns:
            count_without_nulls = self.dataframe.na.drop(subset=column).count()
            total_count = self.dataframe.count()
            null_count = total_count - count_without_nulls
            null_percentage = (null_count / total_count)

            if lower_limit <= null_percentage <= upper_limit:
                print("The column: {0} contains null values within the defined percentage".format(column))
            else:
                print("The column: {0} contains null values that are not within the defined percentage".format(column))
                columns_with_nulls += 1
        
        if columns_with_nulls > 0:
            status = 'Failed'
        else:
            status = 'Passed'
    
        return status

    def check_columns(self, columns):

        """
        Summary: This function is used to check if the columns of the dataframe matches the user defined list of columns

        Parameters: List of columns expected in the dataframe

        Output: Returns the status of the test i.e. Passed or Failed

        """

        dataframe_columns = self.dataframe.columns

        if dataframe_columns == columns:
            print("The expected columns are present in the dataset")
            status = 'Passed'
        else:
            missing_columns = [items for items in dataframe_columns if items not in columns]
            print("There are columns which do not match the user defined columns. The list of columns names are: {0}".format(missing_columns))
            status = 'Failed'
        
        return status