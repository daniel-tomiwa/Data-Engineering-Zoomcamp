import pandas as pd
import argparse
from time import time
from sqlalchemy import create_engine


def get_filename(month, year, init_url, service):
    
    # sets the month part of the file_name string
    month = '0'+str(month+1)
    month = month[-2:]

    # csv file_name 
    file_name = f"{service}_tripdata_{year}-{month}.csv.gz'"
    file_url = f"{init_url}/{service}/{file_name}"

    return file_url
        

def upload_to_postgres(i, user, password, host, port, db, csv_name, table_name):
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    
    if i == 1:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print(f"Finished ingesting {csv_name} into the postgres database")
            break

      
def main(params):
    
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    year = params.year
    
    # services = ['fhv','green','yellow']
    service = "yellow"
    init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
    
    for i in range(12):
        
        file_url = get_filename(i, year, init_url, service)  
        upload_to_postgres(i, user, password, host, port, db, file_url, table_name)

    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Ingest local CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--year', required=True, help='year of the data to be ingested')

    args = parser.parse_args()

    main(args)