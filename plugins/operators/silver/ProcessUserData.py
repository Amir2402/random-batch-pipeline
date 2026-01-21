from airflow.sdk import BaseOperator
from include.utils.S3HelperFunctions import S3HelperFunctions
from include.utils.queries import connect_duck_db_to_S3, select_json_from_bronze
from include.utils.S3HelperFunctions import write_deltalaken

def ppppp():
    # in this class we process data coming from the bronze data
    pass
