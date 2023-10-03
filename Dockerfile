FROM python:3.8
WORKDIR /app
COPY "Entregable3.py" .
COPY "credentials.env" .
RUN pip install install finnhub-python
RUN pip install python-dotenv
RUN pip install sqlalchemy
RUN pip install pandas
RUN pip install psycopg2
RUN pip install redshift-connector

CMD [ "python", "-u", "Entregable3.py" ]