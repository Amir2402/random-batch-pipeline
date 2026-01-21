import duckdb 
from deltalake.writer import write_deltalake
from include.config.variables import S3_ACCESS
import os

os.environ["AWS_ALLOW_HTTP"] = "true"

def connect_duck_db_to_S3():
    conn = duckdb.connect()
    
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")
    conn.install_extension("delta")
    conn.load_extension("delta")
    
    conn.execute(f"SET s3_region='us-east-1';")
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS['aws_access_key']}';")
    conn.execute(f"SET s3_secret_access_key='{S3_ACCESS['aws_secret_key']}';")
    conn.execute(f"SET s3_endpoint='{S3_ACCESS['s3_endpoint_duckdb']}';") 
    conn.execute(f"SET s3_use_ssl=false;")
    conn.execute(f"SET s3_url_style='path';")

    return conn

def select_json_from_bronze(table_name, year, month, day):
    select_statement = f"""
        CREATE TABLE {table_name} AS 
            SELECT 
                UNNEST(data) AS data
            FROM 
                read_json('s3://bronze/{table_name}/year={year}/month={month}/day={day}/*.json');
        """
    
    return select_statement