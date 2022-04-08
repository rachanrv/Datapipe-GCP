import datetime  # to assign scheduler
from google.cloud import bigquery # to use big query api in python
from google.cloud import storage  # to ingest and move the files to storage 
from airflow.models import Variable # optional to assign the variables through other environments
import pandas as pd # to create dataframes
#import gcsfs # to connect to Google Cloud Storage  
from airflow import models # to assign tasks in Airflow
from airflow.operators import python_operator # to implement python script in Airflow
from airflow.operators import bash_operator    # To implement linux or cloud commands in Airflow


   # The start date describes when a DAG is valid or can run. Set this to a fixed point in time rather than                                                 
    #dynamically, since it is evaluated every time, a DAG is parsed. 
    #See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
default_dag_args = {
     'start_date': datetime.datetime.today()
}
    # End datetime can also be implemented for scheduling

# Configure variables for Connections in GCP
BQ_CONN_ID = 'my_gcp_conn' # name of the connection created
BQ_PROJECT = 'dataairflow-poc' # project_id of GCP
#for eg, it will be – ‘dataairflow-poc’

# Create a DAG (directed acyclic graph) of tasks
# Any task you create within the context manager is automatically added to the DAG object.
# Replace existing project-id with current project id
# Replace bucket names with current bucket name
with models.DAG(
        'datapipe_poc', # DAG name
        schedule_interval=datetime.timedelta(days=1),  # DAGs processing time interval
        default_args=default_dag_args) as dag:
    def greeting():
        #https://cloud.google.com/resource-manager/docs/creating-managing-projects
        project_id = 'dataairflow-poc'
        client = bigquery.Client(project=BQ_PROJECT)
        # stored procedure 
        query1 = f"""CALL `dataairflow-poc.raw.usp_create_table_raw_make` ();"""
        # for eg, `dataairflow-poc.raw.usp_create_table_raw_make`

        # Set up the query
        query_job = client.query(query1)
        # TO DO(developer): Set table_id to the ID of the table to create.
        table_id = "dataairflow-poc.raw.make"
        # For eg, table_id= "‘dataairflow-poc.raw.make’
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = False
        uri = "gs://v_stage_2022/Make.csv"
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)  
        # Make an API request. Do validate data ingestion
        load_job.result()  # Waiting for the job to complete.
        
        # stored procedure 
        # transform data
        query2 = f"""CALL `dataairflow-poc.raw.usp_transform_raw_stage_make` ();"""
        # for eg, `dataairflow-poc.raw.usp_transform_raw_stage_make`
        # Set up the query
        query_job2 = client.query(query2)
               
        # stored procedure 
        query3 = f"""CALL `dataairflow-poc.stage.usp_upd_ins_stage_base_make` ();"""
        # for eg, `dataairflow-poc.stage.usp_upd_ins_stage_base_make`
        # Set up the query
        query_job3 = client.query(query3)
                         
   # An instance of an operator is called a task. In this case, the
   # BQ_task task calls the "BQ" Python function.
    BQ_task = python_operator.PythonOperator(
        task_id='BQ',
        python_callable=greeting)

   # Create BigQuery output dataset.
    move_staging = bash_operator.BashOperator(
      task_id='move_inbound_dataset',
      # Executing 'bq' command requires Google Cloud SDK which comes
      # preinstalled in Cloud Composer.
      bash_command='gsutil mv gs://v_inbound_2022/Make.csv* gs://v_stage_2022/Make.csv')

   # Create BigQuery output dataset.
    move_archive = bash_operator.BashOperator(
      task_id='move_staging_dataset',
      # Executing 'bq' command requires Google Cloud SDK which comes
      # pre installed in Cloud Composer.
      bash_command='gsutil mv gs://v_stage_2022/Make.csv* gs://v_archive_2022/Make.csv') 
 
move_staging >> BQ_task >> move_archive