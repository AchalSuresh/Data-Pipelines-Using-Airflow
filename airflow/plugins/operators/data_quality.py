from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class DataQualityOperator(BaseOperator):
    '''
    The function performs basic data quality on the dataset
    INPUTS:
    redshift_conn_id : The configurations to connect to the Redshift cluster created. Loaded from the Admin Console on Airflow
    tables: Takes the table on which the data quality is to be run
    test_stmt: test SQL command to check validity of target table        
    result: result of test_stmt to check validity
            
    '''
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 tables=[],
                 test_stmt=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.test_stmt=test_stmt
        
        
    def execute(self, context):
        
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
                num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")


        for stmt in self.test_stmt:
            output = redshift_hook.get_records(stmt.get('test'))
            if stmt.get('expected_result') != output:
                raise ValueError(f"Fail the result, is not matching with the expected value")