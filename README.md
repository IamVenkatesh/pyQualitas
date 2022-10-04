# pyvalidator
This is a project to develop python scripts for validating the data. This project is an inspiration from deequ and 
dataflare which are also aimed towards the QA validations around the data.

**Dependencies:**

1. Pyspark - Version 3.3.0
2. Plotly - Version 5.10.0

**Features:**

1. **Checks:**

This package contains the checks that can be used to ensure the Data Quality. The following are description and 
examples for the various functionality it offers:

* **Single Dataframe Checks:**

This feature allows you to apply certain checks on a single dataframe. The instantiation of the class can be done 
from your script by passing in the dataframe (which has to be validated) as a parameter.

For example let's consider that you have a dataframe called df1. so if you are going to instantiate, it can be done as 
follows:

from src.checks.singledfchecks import SingleDataFrameChecks

single_df_checks = SingleDataFrameChecks(df1)

Now "single_df_checks" will have access to all the methods that are available as part of the class.

Let's go into detail about the methods available and how to use them 

* **check_duplicates**

This function helps us to check if there are any duplicates in the column of a dataframe. The method takes a list of 
columns (for example: ["employee_id", "salary"]) and will check for duplicates in each column individually. 

For example: single_df_checks.check_duplicates(["employee_id"])

The check will return the status as either 'Passed' or 'Failed' (in cases where duplicates are present). The log file 
will have the collection of duplicate values present in that column.

* **check_threshold_count**

This function helps us to check if the total record count in the table is within the defined lower & upper limits.
The method takes the lower limit and upper limit count and checks if the record count in the dataframe is within the 
given limits. 

For example: single_df_checks.check_duplicates(lower_limit=1, upper_limit=5)

The check will return the status as either 'Passed' or 'Failed' (in cases where record count is not within the limits). 
The log file will have the actual record count of the dataframe in case of failure.


