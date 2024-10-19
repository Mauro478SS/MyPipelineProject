from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from etl_commodities import main
from etl_countries import main
from etl_regions import main
from etl_export_records import main
from dim_commodities import main
from dim_countries import main
from dim_regions import main
from ft_export import create_fact_table

def my_function(**kwargs):
    print(f"Current date and time: {datetime.now()}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 16),
    'retries': 1,
}

with DAG(dag_id='DAG_etl_pipeline', default_args=default_args, schedule_interval='0 20 * * *') as dag:
    start = DummyOperator(task_id='start')

    load_stg_commodities = BashOperator(
        task_id='load_staging_stg_commodities',
        bash_command='python /opt/airflow/dags/MyPipelineProject/MyPipelines/etl_commodities.py'
    )

    load_stg_countries = BashOperator(
        task_id='load_staging_stg_countries',
        bash_command='python /opt/airflow/dags/MyPipelineProject/MyPipelines/etl_countries.py'
    )
    
    load_stg_regions = BashOperator(
        task_id='load_staging_stg_regions',
        bash_command='python /opt/airflow/dags/MyPipelineProject/MyPipelines/etl_regions.py'
    )
    
    load_stg_export_records = BashOperator(
        task_id='load_staging_stg_export_records',
        bash_command='python /opt/airflow/dags/MyPipelineProject/MyPipelines/etl_export_records.py'
    )
    
    update_dim_commodities = BashOperator(
        task_id='update_dim_commodities',
        bash_command='python /opt/airflow/dags/MyPipelineProject/MyPipelines/dim_commodities.py'
    )

    update_dim_countries = BashOperator(
        task_id='update_dim_countries',
        bash_command='python /opt/airflow/dags/MyPipelineProject/MyPipelines/dim_countries.py'
    )

    update_dim_regions = BashOperator(
        task_id='update_dim_regions',
        bash_command='python /opt/airflow/dags/MyPipelineProject/MyPipelines/dim_regions.py'
    )

    load_ft_export = BashOperator(
        task_id='Load_ft_export',
        bash_command='python /opt/airflow/dags/MyPipelineProject/MyPipelines/ft_export.py'
    )

    end = DummyOperator(task_id='end')

    # Definir la secuencia de tareas
    start >> load_stg_commodities >> update_dim_commodities 
    start >> load_stg_countries >> update_dim_countries 
    start >> load_stg_regions >> update_dim_regions 
    start >> load_stg_export_records >> load_ft_export >> end