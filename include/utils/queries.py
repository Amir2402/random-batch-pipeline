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

def read_json_from_bronze(table_name, year, month, day):
    read_json = f"""
        CREATE TABLE {table_name} AS 
            SELECT 
                UNNEST(data.results) AS results
            FROM 
                (
                    SELECT 
                        UNNEST(data) AS data
                    FROM 
                        read_json('s3://bronze/{table_name}/year={year}/month={month}/day={day}/*.json')
                );
        """
    
    return read_json

def read_delta_table_query(delta_table_name, duckdb_table_name, minio_access_key, minio_secret_key, bucket_name, current_year, current_month, current_day, minio_endpoint):
    read_delta_table = f"""
        CREATE SECRET secret_minio_{delta_table_name} (
        TYPE S3,
        ENDPOINT '{minio_endpoint}',
        URL_STYLE 'path',
        USE_SSL false,
        KEY_ID {minio_access_key},
        SECRET {minio_secret_key}
        );
        CREATE TABLE {duckdb_table_name} AS 
            SELECT *
            FROM delta_scan('s3://{bucket_name}/{delta_table_name}')
            WHERE year = {current_year} AND month = {current_month} AND day = {current_day};
    """ 

    return read_delta_table

user_transform_silver = """
    CREATE TABLE silver_user_data AS 
        SELECT
            results.login.uuid AS user_id, 
            results.name.first AS firstname, 
            results.name.last AS lastname,
            results.gender AS gender,
            CAST(results.dob.date AS DATE) AS date_of_birth,
            results.location.street.number AS street_number,
            results.location.street.name AS street_name,
            results.location.city AS city,
            results.location.state AS state,
            results.location.country AS country,
            YEAR(current_date()) AS year,
            MONTH(current_date()) AS month, 
            DAY(current_date()) AS day
        FROM
            user_data;
"""

select_user_dimension = """
    CREATE TABLE gold_user_dim AS
        WITH duplicated_users AS (
            SELECT
                user_id,
                firstname,
                lastname,
                gender,
                row_number() OVER(PARTITION BY user_id) as rownum
            FROM
                user_dim
        )
        SELECT
            user_id,
            firstname,
            lastname,
            gender
        FROM
            duplicated_users
        WHERE
            rownum = 1;
"""

select_location_dimension = """
    CREATE TABLE gold_location_dim AS
        WITH duplicated_locations AS (
            SELECT
                street_number,
                street_name,
                lastname,
                city,
                state,
                country,
                MD5(CONCAT(street_number, street_name, lastname, city, state, country)) as location_id,
                row_number() OVER(PARTITION BY location_id) as rownum
            FROM
                location_dim
        )
        SELECT
            location_id
            street_number,
            street_name,
            lastname,
            city,
            state,
            country,
        FROM
            duplicated_locations
        WHERE
            rownum = 1;
"""

select_date_dimension = """
    CREATE TABLE gold_date_dim AS
        WITH duplicated_date AS (
            SELECT
                year,
                month,
                day,
                MD5(CONCAT(year, month, day)) as date_id,
                row_number() OVER(PARTITION BY date_id) as rownum
            FROM
                date_dim
        )
        SELECT
            date_id,
            year,
            month,
            day
        FROM
            duplicated_date
        WHERE
            rownum = 1;
"""