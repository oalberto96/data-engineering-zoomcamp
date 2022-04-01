#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
from time import time



def ingest_callable(user, password, host, port, db, table_name, csv_file):
    print(table_name)
    print(f"Port: {port}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print("Connection established successfully")

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True:
        t_start = time()
        
        try:
            df = next(df_iter)
        except StopIteration:
            print("Completed")
            break
        
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print("Inserted another chunk..., took %.3f second" % (t_end - t_start))
