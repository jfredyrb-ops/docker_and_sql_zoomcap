#!/usr/bin/env python


import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
# import click


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


# @click.command()
# @click.option('--pg-user', default='root', help='PostgreSQL user')
# @click.option('--pg-pass', default='root', help='PostgreSQL password')
# @click.option('--pg-host', default='localhost', help='PostgreSQL host')
# @click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
# @click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
# @click.option('--target-table', default='yellow_taxi_data', help='Target table name')
#
def load_data():
    """Function load data version."""
    pg_user = 'root'
    pg_password = 'root'
    pg_host = 'localhost'
    pg_port = str(5432)
    pg_db = 'ny_taxi'
    year = 2021
    month = 4
    chunk_size = 120000
    target_table = 'yellow_taxi_data'
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunk_size,
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(n=0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            first = False
        print(len(df_chunk))
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )


if __name__ == '__main__':
    load_data()
