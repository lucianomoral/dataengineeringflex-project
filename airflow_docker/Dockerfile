FROM python:3.8
FROM apache/airflow:2.3.3

COPY credentials.env ./

RUN pip install install finnhub-python
RUN pip install python-dotenv
RUN pip install sqlalchemy
RUN pip install pandas
RUN pip install redshift-connector