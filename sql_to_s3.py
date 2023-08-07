import pandas as pd
import pyodbc
import boto3
from io import BytesIO
from fastparquet import write
import argparse
import logging

def connect_to_sql_server(server, database, username, password):
    """Establish connection to SQL Server."""
    conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    return pyodbc.connect(conn_string)

def data_generator(connection, query, chunksize=50000):
    """Generator function to yield data chunks from SQL Server."""
    cursor = connection.cursor()
    offset = 0
    while True:
        chunk_query = f"{query} OFFSET {offset} ROWS FETCH NEXT {chunksize} ROWS ONLY"
        data = pd.read_sql(chunk_query, connection)
        if data.empty:
            break
        yield data
        offset += chunksize
    cursor.close()

def upload_to_s3(data, bucket, path):
    """Upload data to S3 in parquet format."""
    buffer = BytesIO()
    write(buffer, data, file_scheme='hive')
    buffer.seek(0)
    s3 = boto3.client('s3')
    s3.put_object(Body=buffer.getvalue(), Bucket=bucket, Key=path)

def main(args):
    logging.basicConfig(level=logging.INFO)
    
    conn = connect_to_sql_server(args.server, args.database, args.username, args.password)

    query = f"SELECT * FROM {args.table} ORDER BY {args.date_column}"
    
    for chunk in data_generator(conn, query):
        year = chunk[args.date_column].dt.year.unique()[0]
        month = chunk[args.date_column].dt.month.unique()[0]
        day = chunk[args.date_column].dt.day.unique()[0]
        s3_path = f"{args.s3_path}/year={year}/month={month}/day={day}/data.parquet"
        
        logging.info(f"Uploading data for {year}-{month}-{day} to {s3_path}")
        upload_to_s3(chunk, args.bucket, s3_path)

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer data from SQL Server to S3 in Parquet format.")
    parser.add_argument("--server", required=True, help="SQL Server name.")
    parser.add_argument("--database", required=True, help="Database name.")
    parser.add_argument("--username", required=True, help="Username for SQL Server.")
    parser.add_argument("--password", required=True, help="Password for SQL Server.")
    parser.add_argument("--table", required=True, help="Table to fetch data from.")
    parser.add_argument("--date_column", required=True, help="Date column for partitioning.")
    parser.add_argument("--bucket", required=True, help="S3 Bucket name.")
    parser.add_argument("--s3_path", required=True, help="Base path in S3 where data will be stored.")
    
    args = parser.parse_args()
    main(args)

'''
python script_name.py \
--server YOUR_SERVER_NAME \
--database YOUR_DATABASE_NAME \
--username YOUR_USERNAME \
--password YOUR_PASSWORD \
--table your_large_table \
--date_column your_date_column \
--bucket your_s3_bucket \
--s3_path path/to/destination
'''
