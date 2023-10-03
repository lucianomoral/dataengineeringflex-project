This script uses finnhub api to retrieve stock prices and load them into an Amazon Redshift database.
API General documentation: https://finnhub.io/docs/api

Company info api documentation: https://finnhub.io/docs/api/company-profile2

Stock prices api documentation: https://finnhub.io/docs/api/quote

Libraries to install: 
finnhub-python
python-dotenv
sqlalchemy
pandas
psycopg2
redshift_connector