from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    '''
    The constructor function to initialize the arguments
    INPUT:
    redshift_conn_id : The configurations to connect to the Redshift cluster created. Loaded from the Admin Console on Airflow
    tables: Takes the table to which data is to be loaded
    sql: The SQL query that is to be run for each table, retrieved from the helper class SQlQueries
    append_only : Argument which determines if the operation is to append data or to delete content in data and re enter
    
    '''

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append_only="",
                               
                 *args, **kwargs):
    
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id,
        self.table=table,
        self.sql=sql,        
        self.append_only = append_only


    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('LoadDimensionOperator not implemented yet')
        if not self.append_only:
            self.log.info("Delete {} dimension table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Insert data from fact table into {} dimension table".format(self.table))
        formatted_sql = getattr(SqlQueries,self.sql).format(self.table)
        redshift.run(formatted_sql)
                
        