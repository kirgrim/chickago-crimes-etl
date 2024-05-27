import os

DB_SERVER_URL = os.getenv("DB_SERVER_URL", "127.0.0.1:1433")
DB_USERNAME = os.getenv("DB_USERNAME", "SA")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Xyz12345")
SQL_DATABASE = os.getenv("SQL_DATABASE", 'chicago_etl')
SQL_DRIVER = os.getenv("SQL_DRIVER", 'ODBC+Driver+17+for+SQL+Server')
