## Importing necessary libraries

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import time
from utils import DATA_DIR, DB_PATH
from logger import get_logger

## Create logger
logger = get_logger('data_ingestion.log')

## Creating database engine with Sqlalchemy

engine = create_engine(f'sqlite:///{DB_PATH}')

## Data Ingestion Function

def ingest_data_to_db(df, table_name, engine):
    """
    Ingests a DataFrame into a specified table in the database.
    """
    df.to_sql(table_name, con = engine, if_exists = 'replace', index = False)
    print(f"Data ingested into table: {table_name}")
    logger.info(f"Data ingested into table: {table_name}")

## Reading the datasets and storing in SQL database

def load_raw_data():
    """
    Loads all CSV files in the data folder into the SQLite database.
    """
    # Check if the data directory exists
    if not os.path.exists(DATA_DIR):
        raise FileNotFoundError(f"The directory {DATA_DIR} does not exist.")
    
    start_time = time.time()
    logger.info(f"Starting data ingestion from {DATA_DIR}")

    for file in os.listdir(DATA_DIR):
        if file.endswith('.csv'):
            file_path = os.path.join(DATA_DIR, file)

            try:
                df = pd.read_csv(file_path)
                logger.info(f"Ingesting file {file} in database")
                ingest_data_to_db(df, file.split('.')[0], engine)
            
            except Exception as e:
                logger.error(f"Error processing file {file}: {e}")
    
    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f"Data ingestion completed in {total_time:.2f} seconds")

## Main execution block

if __name__ == '__main__':
    load_raw_data()
    print("Data ingestion completed successfully.")
    logger.info("Data ingestion completed successfully.")