## Importing necessary libraries

import os
import numpy as np
import pandas as pd
import sqlite3
import time
from sqlalchemy import create_engine
from utils import DB_PATH
from logger import get_logger
from data_ingestion import ingest_data_to_db

## Create Logger
logger = get_logger('get_vendor_summary.log')

## SQL query for getting vendor_summary

def get_vendor_summary(connection):
    """
    This function will merge the different tables to get the overall vendor summary.
    """
    query = """
        WITH
        
        FreightSummary AS (
            SELECT
                VendorNumber, SUM(Freight) AS FreightCost
            FROM
                vendor_invoice
            GROUP BY
                VendorNumber
        ),
        
        PurchaseSummary AS (
            SELECT
                p1.VendorNumber, p1.VendorName, p1.Brand, p1.Description, p1.PurchasePrice,
                SUM(p1.Quantity) AS TotalPurchaseQuantity,
                SUM(p1.Dollars) AS TotalPurchaseDollars,
                p2.Price AS ActualPrice, p2.Volume
            FROM
                purchases p1
            JOIN
                purchase_prices p2
            ON
                p1.Brand = p2.Brand
            WHERE
                p1.PurchasePrice > 0
            GROUP BY
                p1.VendorNumber, p1.VendorName, p1.Brand, p1.Description, p1.PurchasePrice, p2.Price, p2.Volume

        ),
        
        SalesSummary AS (
            SELECT
                VendorNo, Brand,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(SalesDollars) AS TotalSalesDollars,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(ExciseTax) AS TotalExciseTax
            FROM
                sales
            GROUP BY
                VendorNo, Brand
        )
        
        SELECT
            ps.VendorNumber, ps.VendorName, ps.Brand, ps.Description, ps.PurchasePrice, ps.ActualPrice, ps.Volume,
            ps.TotalPurchaseQuantity, ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity, ss.TotalSalesDollars, ss.TotalSalesPrice, ss.TotalExciseTax,
            fs.FreightCost
        FROM
            PurchaseSummary ps
        LEFT JOIN
            SalesSummary ss
        ON
            (ps.VendorNumber = ss.VendorNo) AND (ps.Brand = ss.Brand)
        LEFT JOIN
            FreightSummary fs
        ON
            ps.VendorNumber = fs.VendorNumber
        ORDER BY
            ps.TotalPurchaseDollars DESC
    """

    return pd.read_sql_query(query, connection)

## Data Cleaning

def clean_data(df):
    """
    Function to be used for cleaning the data.
    """

    ## Setting correct datatype for 'Volume' column
    df['Volume'] = df['Volume'].astype('float64')

    ## Filling missing values in Sales related columns
    df.fillna(0, inplace = True)

    ## Removing unnecessary whitespaces
    df['VendorName'] = df['VendorName'].str.strip()

    ## Creating new columns for better analysis

    # Gross Profit
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']

    # Profit Margin
    df['ProfitMargin'] = df['GrossProfit'] / df['TotalSalesDollars'] * 100

    # Stock Turnover
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']

    # Sales to Purchase Ratio
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    return df


## Main function

if __name__ == '__main__':

    ## Creating database connection

    connection = sqlite3.connect(DB_PATH)
    engine = create_engine(f'sqlite:///{DB_PATH}')

    ## Creating vendor summary

    start_time = time.time()

    logger.info('Creating Vendor Summary Table...')
    summary_df = get_vendor_summary(connection)
    logger.info('Vendor Summary Table Created!')

    logger.info('Cleaning data...')
    cleaned_df = clean_data(summary_df)
    logger.info('Data Cleaned!')

    logger.info('Ingesting data to database...')
    ingest_data_to_db(cleaned_df, 'vendor_summary', engine)
    logger.info('Data ingested to database!')

    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f'Total time taken: {total_time:.2f} seconds')