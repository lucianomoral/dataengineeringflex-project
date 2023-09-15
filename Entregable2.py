# Libraries import
import finnhub
import pandas as pd
import json
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('credentials.env')
load_dotenv(dotenv_path=dotenv_path)

# DB Connection and tables creation
host = os.getenv('HOST')
port = os.getenv('PORT')
db = os.getenv('DB')
user = os.getenv('USER')
pwd = os.getenv('PWD')

connection_string = 'postgresql+psycopg2://' + user + ':' + pwd + '@' + host + ":" + port + "/" + db
  
engine = create_engine(connection_string)

conn = engine.connect()

# ticker sentiment table creation
sql = "CREATE TABLE IF NOT EXISTS lucianof_moral_coderhouse.ticker_sentiment ( ticker varchar NOT null, ticker_name varchar NOT NULL, country varchar NOT NULL, exchange varchar NOT NULL, currency varchar NOT NULL, industry varchar NOT NULL, ipo date null, mention integer NOT NULL, positive_score decimal(18,5) NOT NULL, negative_score decimal(18,5) NOT NULL, positive_mention integer NOT NULL, negative_mention integer NOT NULL, score decimal(18,5) NOT NULL, sentiment_source varchar NOT NULL, sentiment_date date NOT NULL, sentiment_time time NOT NULL, CONSTRAINT newtable_pk PRIMARY KEY (ticker, sentiment_time, sentiment_date) )"

conn.execute((text(sql)))

# API consumption for set of tickers

secret_key = os.getenv('KEY')

finnhub_client = finnhub.Client(api_key=secret_key)

# Companies that will be tracked are defined within array

companies_to_track = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META', 'V', 'XOM', 'WMT', 'JPM']

# Array that will be loaded into DataFrame

companies = []

companies_sentiment = []

# Loop each symbol and add that info to array
for ticker in companies_to_track:
  companies.append(finnhub_client.company_profile2(symbol=ticker))
  for record in finnhub_client.stock_social_sentiment(ticker, '2023-09-01', '2023-09-02')['reddit']:
    record['ticker'] = ticker
    record['sentiment_source'] = 'reddit'
    companies_sentiment.append(record)

# Load companies array into DataFrame
df_companies = pd.DataFrame.from_records(companies)

# Drop other columns that won't be used
df_companies = df_companies[['ticker','name', 'country', 'currency', 'exchange', 'finnhubIndustry', 'ipo']]

# Rename of columns
df_companies = df_companies.rename(columns={"finnhubIndustry":"industry", "name":"ticker_name"})

# Load sentiment data array into DataFrame
df_sentiment = pd.DataFrame.from_records(companies_sentiment)

# Split of datetime column into date and time
df_sentiment['sentiment_date'] = pd.to_datetime(df_sentiment['atTime']).dt.date
df_sentiment['sentiment_time'] = pd.to_datetime(df_sentiment['atTime']).dt.time

# Drop datetime column
df_sentiment = df_sentiment.drop('atTime', axis=1)

# Renaming of columns to match database column names
df_sentiment = df_sentiment.rename(columns={"positiveScore": "positive_score", "negativeScore": "negative_score",  "positiveMention": "positive_mention", "negativeMention": "negative_mention"})

# Merge of both datasets into a single one
df = pd.merge(df_companies, df_sentiment, 'inner', 'ticker')

# For each ticker sentiment + date + time I check if it is already created or not, and depending on that I insert the record or skip it
for i in range(len(df)):

  sql = "SELECT ticker FROM ticker_sentiment WHERE ticker ='{}' AND sentiment_time = '{}' AND sentiment_date = '{}'".format(df.iloc[i]['ticker'], df.iloc[i]['sentiment_time'], df.iloc[i]['sentiment_date'])

  result = conn.execute((text(sql)))
  
  row_values = df.iloc[i].to_dict()

  if result.rowcount == 0:
    sql = """INSERT INTO ticker_sentiment VALUES (
                '{ticker}',
                '{ticker_name}',
                '{country}',
                '{exchange}',
                '{currency}',
                '{industry}',
                '{ipo}',
                '{mention}',
                '{positive_score}',
                '{negative_score}', 
                '{positive_mention}', 
                '{negative_mention}',
                '{score}', 
                '{sentiment_source}',
                '{sentiment_date}',
                '{sentiment_time}'
                )""".format(**row_values)
    conn.execute((text(sql)))