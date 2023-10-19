# Libraries import
from dotenv import load_dotenv
from pathlib import Path
from email import message
import finnhub
import pandas as pd
import json
import os
import datetime
import redshift_connector
import smtplib

def load_dotenv_file():
    dotenv_path = Path('credentials.env')
    load_dotenv(dotenv_path=dotenv_path)


def connect_to_database():
  load_dotenv_file()

  # DB Connection and tables creation
  host = os.getenv('HOST')
  port = os.getenv('PORT')
  db = os.getenv('DB')
  user = os.getenv('USER')
  password = os.getenv('PASSWORD')

  return redshift_connector.connect(
      host=host,
      database=db,
      port=int(port),
      user=user,
      password=password
      )


def send_email():

    load_dotenv_file()

    email_address_from = os.getenv('EMAIL_ADDRESS_FROM')
    email_address_to = os.getenv('EMAIL_ADDRESS_TO')
    email_password = os.getenv('EMAIL_PASSWORD')

    conn = connect_to_database()

    sql = "SELECT ticker, ticker_name FROM ticker_prices WHERE record_creation_date = '{}'".format(datetime.datetime.now())
    
    cursor = conn.cursor()

    cursor.execute(sql)

    result: tuple = cursor.fetchall()

    body_text=''

    if not result:
       body_text='En el día de hoy, no se actualizó ningún precio.'
    else:
       body_text='En el día de hoy, se actualizaron los precios de los siguientes tickers:\n'
       for ticker in result:
          body_text += ticker[0] + ' - ' + ticker[1] + '\n'

    try:
        server=smtplib.SMTP('smtp.gmail.com',587)
        server.starttls()
        server.login(email_address_from,email_password)
        subject='Tickers actualizados hoy'
        message='Subject: {} \n\n {}'.format(subject,body_text)
        server.sendmail(email_address_from,email_address_to,message.encode('utf-8'))
        print('Email sent succesfully')
    except Exception as exception:
        print(exception)
        print('Email was not sent')


def pushStockPricesIntoRedshiftDB():

  load_dotenv_file()

  conn = connect_to_database()

  # ticker sentiment table creation
  sql = "CREATE TABLE IF NOT EXISTS lucianof_moral_coderhouse.ticker_prices ( ticker varchar NOT null, ticker_name varchar NOT NULL, country varchar NOT NULL, exchange varchar NOT NULL, currency varchar NOT NULL, industry varchar NOT NULL, ipo date null, price_date date NOT NULL, current_price decimal(18,5) NOT NULL, change decimal(18,5) NOT NULL, percent_change decimal(18,5) NOT NULL, high decimal(18,5) NOT NULL, low decimal(18,5) NOT NULL, open_price decimal(18,5) NOT NULL, previous_close_price decimal(18,5) NOT NULL, record_creation_date date NOT NULL, CONSTRAINT newtable_pk PRIMARY KEY (ticker, price_date) )"

  # Create a Cursor object
  cursor = conn.cursor()

  cursor.execute(sql)

  conn.commit()

  # API consumption for set of tickers

  secret_key = os.getenv('KEY')

  finnhub_client = finnhub.Client(api_key=secret_key)

  # Companies that will be tracked are defined within array

  companies_to_track = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META', 'V', 'XOM', 'WMT', 'JPM']

  # Array that will be loaded into DataFrame

  companies = []

  companies_prices = []

  # Loop each symbol and add that info to array
  for ticker in companies_to_track:
    companies.append(finnhub_client.company_profile2(symbol=ticker))
    quote = finnhub_client.quote(ticker)
    record = {}
    record["ticker"] = ticker
    record["price_date"] = datetime.datetime.utcfromtimestamp(quote['t']).strftime('%Y-%m-%d')
    record["current_price"] = quote['c']
    record["change"] = quote['d']
    record["percent_change"] = quote['dp']
    record["high"] = quote['h']
    record["low"] = quote['l']
    record["open_price"] = quote['o']
    record["previous_close_price"] = quote['pc']
    record['record_creation_date'] = datetime.datetime.now()
    companies_prices.append(record)

  # Load companies array into DataFrame
  df_companies = pd.DataFrame.from_records(companies)

  # Drop other columns that won't be used
  df_companies = df_companies[['ticker','name', 'country', 'currency', 'exchange', 'finnhubIndustry', 'ipo']]

  # Rename of columns
  df_companies = df_companies.rename(columns={"finnhubIndustry":"industry", "name":"ticker_name"})

  # Load sentiment data array into DataFrame
  df_prices = pd.DataFrame.from_records(companies_prices)

  # Merge of both datasets into a single one
  df = pd.merge(df_companies, df_prices, 'inner', 'ticker')

  # For each ticker price + date + time I check if it is already created or not, and depending on that I insert the record or skip it
  for i in range(len(df)):

    sql = "SELECT ticker FROM ticker_prices WHERE ticker ='{}' AND price_date = '{}'".format(df.iloc[i]['ticker'], df.iloc[i]['price_date'])
    
    cursor = conn.cursor()

    cursor.execute(sql)

    result: tuple = cursor.fetchall()

    row_values = df.iloc[i].to_dict()

    if not result:
      sql = """INSERT INTO ticker_prices VALUES (
                  '{ticker}',
                  '{ticker_name}',
                  '{country}',
                  '{exchange}',
                  '{currency}',
                  '{industry}',
                  '{ipo}',
                  '{price_date}',
                  '{current_price}',
                  '{change}', 
                  '{percent_change}', 
                  '{high}',
                  '{low}', 
                  '{open_price}',
                  '{previous_close_price}',
                  '{record_creation_date}'
                  )""".format(**row_values)

      cursor = conn.cursor()

      cursor.execute(sql)

      conn.commit()