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
                UNNEST(sales_data) as sales_data
            FROM 
                read_json('s3://bronze/{table_name}/year={year}/month={month}/day={day}/*.json');
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

sales_transform_silver = """
    CREATE TABLE silver_sales_data AS 
        SELECT
            sales_data.id AS user_id, 
            sales_data.first_name AS firstname, 
            sales_data.last_name AS lastname,
            sales_data.gender AS gender,
            sales_data.job AS job,
            sales_data.ipv4 as ip_address,
            CAST(sales_data.dob AS DATE) AS date_of_birth,
            sales_data.address AS address,
            sales_data.street_address AS street_address,
            sales_data.city AS city,
            sales_data.state AS state,
            sales_data.postal_code AS postal_code,
            sales_data.country as country,
            sales_data.product_id AS product_id,
            sales_data.product_name AS product_name,
            sales_data.unit_price AS unit_price,
            sales_data.quantity AS quantity,
            CAST(sales_data.event_ts AS DATE) AS event_ts,
            YEAR(sales_data.event_ts) AS year,
            MONTH(sales_data.event_ts) AS month, 
            DAY(sales_data.event_ts) AS day,
            MD5(CONCAT(address, street_address, postal_code, city, state, country)) as location_id,
            MD5(CONCAT(year, month, day)) as date_id
        FROM
            sales_data;
"""

select_user_dimension = """
    CREATE TABLE gold_user_dim AS
        WITH duplicated_users AS (
            SELECT
                user_id,
                firstname,
                lastname,
                gender,
                job,
                ip_address,
                date_of_birth,
                row_number() OVER(PARTITION BY user_id) as rownum
            FROM
                user_dim
        )
        SELECT
            user_id,
                firstname,
                lastname,
                gender,
                job,
                ip_address,
                date_of_birth
        FROM
            duplicated_users
        WHERE
            rownum = 1;
"""

select_location_dimension = """
    CREATE TABLE gold_location_dim AS
        WITH duplicated_locations AS (
            SELECT
                location_id,
                address,
                street_address,
                city,
                state,
                country,
                postal_code,
                row_number() OVER(PARTITION BY location_id) as rownum
            FROM
                location_dim
        )
        SELECT
            location_id,
            address,
            street_address,
            city,
            state,
            country,
            postal_code,
        FROM
            duplicated_locations
        WHERE
            rownum = 1;
"""

select_date_dimension = """
    CREATE TABLE gold_date_dim AS
        WITH duplicated_date AS (
            SELECT
                date_id,
                year,
                month,
                day,
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

select_product_dimension = """
    CREATE TABLE gold_product_dim AS
        WITH duplicated_product AS (
            SELECT 
                product_id,
                product_name,
                unit_price,
                row_number() OVER(PARTITION BY product_id) as rownum
            FROM
                product_dim
        )
        SELECT
            *
        FROM
            duplicated_product
        WHERE
            rownum = 1;
"""

select_sales_fact = """
    CREATE TABLE gold_sales_fact AS 
        WITH duplicated_fact AS (
            SELECT 
                user_id,
                product_id,
                date_id,
                location_id,
                quantity,
                unit_price,
                unit_price * quantity AS amount_spent,
                row_number() OVER(PARTITION BY user_id, product_id, date_id, location_id) as rownum
            FROM
                sales_fact
        )
        SELECT 
            *
        FROM
            duplicated_fact
        WHERE
            rownum = 1; 
"""