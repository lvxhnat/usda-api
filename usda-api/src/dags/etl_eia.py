from datetime import datetime, timedelta
from airflow.decorators import dag, task
from usda_api.scrapers.EIA.eia import EIA

default_args = {
    'owner': 'jy',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=0.1)  # Changed 'retry_interval' to 'retry_delay'
}

@dag(dag_id='etl_eia', default_args=default_args, catchup=False, schedule=None)
def etl_eia(): 
    @task(task_id=f'extract_and_transform_eia')
    def extract_and_transform_eia():
        eia_obj = EIA()
        path = eia_obj.extract_and_transform_eia()
        return path


    @task(task_id=f'load_eia')
    def load_eia(path):
        eia_obj = EIA()
        return eia_obj.load_eia(path)

    path = extract_and_transform_eia()
    load_eia(path)

etl_eia_dag = etl_eia()