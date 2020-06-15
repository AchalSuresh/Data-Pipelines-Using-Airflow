from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

#Default argumnets for the dag based on project specifications
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past' : False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup_by_default': False
    
}


#Creating a DAG 
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )





#Start Dummy Operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)




#The opeartor moves log data from S3 and loading into staging tables on redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    data_format = "JSON"
    
)

#The operator moves song data from S3 and loading into staging tables on redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    data_format = "JSON"
)

#The operator loads data from the staging table and creates and loads the songplay(facts) table on redshift
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplay",
    sql="songplay_table_insert",
    append_only=False
)

#The operator loads data from the staging table and creates and loads the user(dim) table on redshift
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql="user_table_insert",
    append_only=False
)

#The operator loads data from the staging table and creates and loads the song(dim) table on redshift
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="song",
    sql="song_table_insert",
    append_only=False
)

#The operator loads data from the staging table and creates and loads the artist(dim) table on redshift
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artist",
    sql="artist_table_insert",
    append_only=False
)

#The operator loads data from the staging table and creates and loads the time(dim) table on redshift
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql="time_table_insert",
    append_only=False
    
)

#The operator to perform quality check on the data

data_quality = DataQualityOperator(
    task_id="data_quality_check_on_tables",
    redshift_conn_id="redshift",
    tables = ['songplay','users','artist','time','songs'],
    
    test_stmt =[{"test":" SELECT COUNT(*) from artist" , "expected_result":100} , 
          {"test":" SELECT COUNT(*) from users" , "expected_result":300}],
    dag=dag
)

#End dummy operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Configuring the task dependencies 
start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table


load_songplays_table >> load_user_dimension_table 
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table 
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> data_quality 
load_song_dimension_table >> data_quality
load_artist_dimension_table >> data_quality 
load_time_dimension_table >> data_quality

data_quality >> end_operator
