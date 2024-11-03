###
###     functions for other scripts
###

### setup

import pandas as pd
import pandas.io.sql as sqlio
import yfinance as yf
import pandas_ta as ta
from io import StringIO

### features

def calculate_technical_indicators(data):
    """
    calculates some technical indicators from yahoo OHLCV data
    """
    # Moving Averages
    data['SMA_20'] = ta.sma(data['Close'], length=20)
    data['EMA_20'] = ta.ema(data['Close'], length=20)
    # Bollinger Bands
    bbands = ta.bbands(data['Close'], length=20)
    data = pd.concat([data, bbands], axis=1)
    # Relative Strength Index
    data['RSI_14'] = ta.rsi(data['Close'], length=14)
    # Moving Average Convergence Divergence
    macd = ta.macd(data['Close'])
    data = pd.concat([data, macd], axis=1)
    return data



### db management


class LoaderException(Exception):
    """
    simple pass through class for loader error management
    """
    pass


def load_technical(ticker, conn, start_date='2017-01-01', end_date='2023-12-31'):
    """
    downloads data for a given ticker and loads into database
    """
    cursor = conn.cursor()

    # getting data
    try:
        data = yf.download(ticker, start=start_date, end=end_date)
        if data.empty:
            raise LoaderException("Data is empty!")
        data.reset_index(inplace=True)
        data.columns = data.columns.get_level_values(0)
        # Calculate returns
        data['Daily Return'] = data['Close'].pct_change()
        data['Adjusted Daily Return'] = data['Adj Close'].pct_change()
        # Add company and ticker info
        data['Company'] = company
        data['Ticker'] = ticker
        # Calculate technical indicators
        data = calculate_technical_indicators(data)
        print(f"Technical data fetched for {ticker}")

    except Exception as e:
        print(f"Error fetching technical data for{ticker}: {e}")


    # writing to database
    csv = StringIO
    data.to_csv(csv)
    csv.seek(0)

    try:
        table = f"{ticker}_technical"
        cursor.execute(f"CREATE OR REPLACE TABLE {table}")
        cursor.copy_from(csv, table)
        cursor.commit()
        return 0

    except (Exception, psycopg.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.rollback()
        cursor.close()
        return 1

