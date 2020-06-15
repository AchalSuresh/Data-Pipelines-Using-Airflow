from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''
    The constructor function to initialize the arguments
    INPUT:
    redshift_conn_id : The configurations to connect to the Redshift cluster created. Loaded from the Admin Console on Airflow
    table: Takes the table to which data is to be loaded
    aws_credentials_id : The key and secret to IAM Role being utilized
    S3_bucket : Determines the bucket name in the S3
    s3_key : Folder name under the S3 bucket
    region : Region of s3 bucket
    data_format: field that allows data to load timestamped files from S3 based on the execution time and run backfills.
    
    '''
    ui_color = '#358140'
     
    
    #SQL statements to load data from staging into redshift
    copy_sql = '''
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT as '{}' 'AUTO'
    
    '''
    
    copy_sql_date = '''
        COPY {}
        FROM '{}/{}/{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT as '{}' 'AUTO'
    
    '''
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region= "us-west-2",
                 data_format = "",
                 json_path="None",
                 *args, **kwargs):
    
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.data_format = data_format
        self.execution_date = kwargs.get('execution_date')
        self.json_path=json_path
        
    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        if not (self.file_format == 'csv' or self.file_format == 'json'):
        raise ValueError(f"file format {self.file_format} is not csv or json")
        if self.file_format == 'json':

            file_format = "format json '{}'".format(self.json_path)
        else:
            file_format = "format CSV"
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        if self.execution_date:
            formatted_sql =StageToRedshiftOperator.copy_sql_date.format(
                self.table, 
                self.s3_path, 
                self.execution_date.strftime("%Y"),
                self.execution_date.strftime("%d"),
                credentials.access_key,
                credentials.secret_key, 
                self.region,
                self.data_format,
                self.execution_date
            )
        else:
            formatted_sql = S3ToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.data_format,
                self.execution_date
            )
        redshift.run(formatted_sql)




