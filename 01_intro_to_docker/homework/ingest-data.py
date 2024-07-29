#Cleaned up version of data-loading.ipynb
import argparse, os, sys
from time import time
import pandas as pd 
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name1 = params.table_name1
    table_name2 = params.table_name2
    url1 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz'
    url2 = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    csv2 = 'zones.csv'

    if url1.endswith('.csv.gz'):
        csv1 = 'trips.csv.gz'
    else:
        csv1 = 'trips.csv'

    # download the CSV files from website
    os.system(f"wget {url1} -O {csv1}")  
    os.system(f"wget {url2} -O {csv2}")
    print(f'download success')
    # Create SQL engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    # Read file based on csv.gz
    df_iter = pd.read_csv(csv1, iterator=True, chunksize=100000, low_memory=False)
    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    # Create the table from header
    df.head(0).to_sql(name=table_name1, con=engine, if_exists='replace')

    
    # Insert values
    t_start = time()
    count = 0
    for batch in df_iter:
        count += 1
        print(f'Inserting batch {count}...')
        b_start = time()
        
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
        batch.to_sql(name=table_name1, con=engine, if_exists='append')
        
        b_end = time()
        print(f'Inserted! Time taken: {b_end - b_start:0.3f} seconds.\n')

        
    t_end = time()   
    print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.')    

    # insert csv2
    print(f'Downloading {table_name2} ...')
    t2_start = time()
    df = pd.read_csv(csv2)
    df.head(0).to_sql(name=table_name2, con=engine, if_exists='replace')
    df.to_sql(name=table_name2, con=engine, if_exists='append')
    t2_end = time()
    print(f'Completed {table_name2}. Time taken {t2_end-t2_start:0.3f} seconds. \n')

if __name__ == '__main__':
    #Parsing arguments 
    parser = argparse.ArgumentParser(description='Loading data from .paraquet file link to a Postgres datebase.')

    parser.add_argument('--user', help='Username for Postgres.')
    parser.add_argument('--password', help='Password to the username for Postgres.')
    parser.add_argument('--host', help='Hostname for Postgres.')
    parser.add_argument('--port', help='Port for Postgres connection.')
    parser.add_argument('--db', help='Databse name for Postgres')
    parser.add_argument('--table_name1', help='name of the table for trips')
    parser.add_argument('--table_name2', help='name of the table for zones')

    args = parser.parse_args()
    main(args)