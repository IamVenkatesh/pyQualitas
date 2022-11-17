from pyspark.sql import SparkSession
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import datetime, os, smtplib
from slack_sdk import WebClient
from slack_sdk.errors import SlackClientConfigurationError
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders
from pymsteams import connectorcard


class Helper:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def create_hive_table_df(self, database_name, table_name):
        """
        Summary: This method is a helper to read the hive database table to spark dataframe

        Parameters: Database Name & Table Name

        Output: Returns a spark dataframe
        """
        return self.spark.table("{0}.{1}".format(database_name, table_name))

    def create_hdfs_files_df(self, format_type, hdfs_location, infer_schema=True):
        """
        Summary: This method is a helper to read the hdfs files to spark dataframe

        Parameters: HDFS File Format, HDFS File Location, True or False for inferSchema. By Default the values is
        set to true.

        Output: Returns a spark dataframe
        """
        return self.spark.read.format(format_type).option("header", True) \
            .option("inferSchema", infer_schema).load(hdfs_location)

    def create_dataframe(self, dataframe, schema):
        """
        Summary: This method is a helper to create a spark dataframe

        Parameters: Data on which the dataframe has to be created, schema of the data

        Output: Returns a spark dataframe
        """
        return self.spark.createDataFrame(data=dataframe, schema=schema)

    @staticmethod
    def generate_report_csv(test_results, file_location):
        """
        Summary: This static method is a helper to save the test results in a csv file
        
        Parameters: The output from the checksuite method, location where the csv file has to be saved. For example: /home/pyqualitas/TestResults.csv
        
        Output: A csv file written to the user defined location
        """
        results = pd.DataFrame(data=test_results, columns=["TestName", "TestDescription", "Status"])
        return results.to_csv(file_location, index=False)

    @staticmethod
    def generate_html_report(test_results, file_location):
        """
        Summary: This static method is a helper to save the test results in a html file
        
        Parameters: The output from the checksuite method, location where the HTML file has to be saved. For example: /home/pyqualitas/TestResults.html
        
        Output: A html file written to the user defined location
        """
        test_result_df = pd.DataFrame(data=test_results, columns=["TestName", "TestDescription", "Status"])
        results_table = test_result_df.to_html(index=False)
        total_test_count = len(test_result_df)
        total_pass_count = len(test_result_df[test_result_df['Status'] == 'Passed'])
        total_fail_count = len(test_result_df[test_result_df['Status'] == 'Failed'])
        template_file_location = os.path.join(os.path.dirname(__file__), 'template')
        env = Environment(loader=FileSystemLoader(template_file_location))
        template = env.get_template('TestResult.html')
        html = template.render(results_table=results_table, total_test_count=total_test_count,
                               total_pass_count=total_pass_count, total_fail_count=total_fail_count,
                               report_time=datetime.datetime.now())
        with open(file_location, "w") as file:
            file.write(html)
        file.close()
    
    @staticmethod
    def slack_notification(channel_name, message, file_location, token_env_variable_name):
        """
        Summary: This static method is a helper to send notifications to slack channels. The pre-requisite
        for using this method is to save the Bot OAuth token as an environment variable and supply the 
        name as an argument. This is done to prevent users from saving/accessing tokens from files.

        Parameters: Channel Name in slack with hash, Message to display, Files to upload as attachments,
        Name of the environment variable where Bot OAuth token is saved
        """
        if os.environ[token_env_variable_name] is not None:
            try:
                client = WebClient(token=os.environ[token_env_variable_name])
            except:
                raise SlackClientConfigurationError
            else:
                client.chat_postMessage(channel=channel_name, text=message)
                if file_location is not None:
                    for items in file_location:
                        client.client.files_upload_v2(channels=channel_name, 
                                                      title="PyQualitas Test Results", 
                                                      file=items, 
                                                      initial_comment="Attachment below:")
        pass
        
    @staticmethod
    def ms_teams_notification(webhook_url, message, color="00AA5A"):
        """
        Summary: Send MS Teams Notification

        Parameters: 
        webhook_url: Incoming webhook url configured in MS Teams channel
        message: The message in string format which has to be displayed
        color: Default is Green. Can be overwritten according to user's taste
        """
        msteams_message = connectorcard(hookurl=webhook_url)
        msteams_message.title("PyQualitas Test Notification")
        msteams_message.text(message)
        msteams_message.color(color)
        msteams_message.send()
        pass
        

    @staticmethod
    def email_notification(send_from, send_to, subject, message, files, 
                           server, port, username, password, use_tls=True):
        """
        Summary: Send Email notifications with attachments

        Parameters: 
            send_from (str): from name
            send_to (list[str]): to name(s)
            subject (str): message title
            message (str): message body
            files (list[str]): list of file paths to be attached to email
            server (str): mail server host name
            port (int): port number
            username (str): server auth username
            password (str): server auth password
            use_tls (bool): use TLS mode
        """
        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = COMMASPACE.join(send_to)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject

        msg.attach(MIMEText(message))

        for path in files:
            part = MIMEBase('application', "octet-stream")
            with open(path, 'rb') as file:
                part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment; filename={}'.format(Path(path).name))
            msg.attach(part)

        smtp = smtplib.SMTP(server, port)
        if use_tls:
            smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.quit()
        pass
