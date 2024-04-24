from datetime import datetime, timedelta
from airflow.decorators import dag, task
from usda_api.scrapers.ESR.esr import ESR

default_args = {
    'owner': 'jy',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=0.1)  # Changed 'retry_interval' to 'retry_delay'
}

@dag(dag_id='etl_esr', default_args=default_args, catchup=False, schedule=None)
def etl_esr(): 
    @task(task_id=f'extract_and_transform_esr')
    def extract_and_transform_esr():
        esr_obj = ESR()
        path = esr_obj.extract_and_transform_esr()
        return path


    @task(task_id=f'load_esr')
    def load_eia(path):
        esr_obj = ESR()
        return esr_obj.load_esr(path)

    path = extract_and_transform_esr()
    load_eia(path)

etl_esr_dag = etl_esr()