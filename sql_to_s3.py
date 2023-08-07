import pandas as pd
import pyodbc
import boto3
from io import BytesIO
from fastparquet import write

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

def main():
    # SQL Server connection details
    server = 'YOUR_SERVER_NAME'
    database = 'YOUR_DATABASE_NAME'
    username = 'YOUR_USERNAME'
    password = 'YOUR_PASSWORD'
    
    conn = connect_to_sql_server(server, database, username, password)

    # Query to fetch data
    query = "SELECT * FROM your_large_table ORDER BY your_date_column"
    
    # Iterate through data chunks
    for chunk in data_generator(conn, query):
        year = chunk['your_date_column'].dt.year.unique()[0]
        month = chunk['your_date_column'].dt.month.unique()[0]
        day = chunk['your_date_column'].dt.day.unique()[0]
        s3_path = f"path/to/destination/year={year}/month={month}/day={day}/data.parquet"
        
        upload_to_s3(chunk, 'your_s3_bucket', s3_path)

    conn.close()

if __name__ == "__main__":
    main()
