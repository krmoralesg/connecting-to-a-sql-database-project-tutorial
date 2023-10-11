import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

def execute_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql = file.read()
        engine.execute(sql)
        print(f'Successfully executed SQL from {file_path}')
        

def connect():
    global engine # Esto nos permite usar una variable global llamada motor
    # Una "cadena de conexión" es básicamente una cadena que contiene todas las credenciales de la base de datos juntas
    connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    print("Starting the connection...")
    engine = create_engine(connection_string)
    engine.connect()
    return engine
# load the .env file variables
load_dotenv()

# 1) Connect to the database here using the SQLAlchemy's create_engine function
engine = connect()
# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function
execute_sql_file('./src/sql/create.sql')
# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function
execute_sql_file('./src/sql/insert.sql')
# 4) Use pandas to print one of the tables as dataframes using read_sql function
sql_query = 'SELECT * FROM books'
df = pd.read_sql(sql_query, con=engine)
print(df.head())
execute_sql_file('./src/sql/drop.sql')