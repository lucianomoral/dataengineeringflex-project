# GENERAL:

This script uses finnhub api to retrieve stock prices and load them into an Amazon Redshift database.
API General documentation: https://finnhub.io/docs/api

Company info api documentation: https://finnhub.io/docs/api/company-profile2

Stock prices api documentation: https://finnhub.io/docs/api/quote

Libraries to install: 
> finnhub-python
> python-dotenv
> sqlalchemy
> pandas
> redshift_connector


# RUNNING INSTRUCTIONS:

1) Clone this repo into local (docker must be installed).
2) Modify ```credentials.env``` file. Define appropiate ```EMAIL_ADDRESS_FROM``` (email address from which an email will be sent with status of process), ```EMAIL_ADDRESS_TO``` (email address to which an email will be sent with status of process) and ```EMAIL_PASSWORD``` (password to login into ```EMAIL_ADDRESS_FROM```) variables. Must be Gmail account. Password has to be generated here: https://myaccount.google.com/apppasswords
3) Open terminal.
4) Move to repo.
5) While in "airflow_docker" folder run this command: ```docker-compose up airflow_init```.
6) Wait for previous command to run.
7) Run this command: ```docker-compose up```.
8) Wait for previous command to run.
9) Open browser and navigate to "localhost:8080/". Login with airflow user (same password as username).
10) Activate DAG.
11) If it does not run automatically, press "Play" button and then "Trigger DAG".
12) Check email was sent and database table updated.