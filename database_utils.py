import pyodbc
import os
import pandas as pd

def connect_DB(server_name: str, database_name: str) -> pyodbc.Connection:
    #if os.getenv("MSI_SECRET"):
    #    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
    #            + 'SERVER={};'.format(server_name)
    #            + 'DATABASE={};'.format(database_name)
    #            + 'Authentication=ActiveDirectoryMsi')
    #else:
    password = os.getenv('DBpassword')
    username = 'agresso-adm'
    driver = '{ODBC Driver 17 for SQL Server}' if os.getenv("MSI_SECRET") else '{SQL Server}'
    cnxn = pyodbc.connect('DRIVER={};'.format(driver)
        + 'SERVER={};'.format(server_name)
        + 'DATABASE={};'.format(database_name)
        + 'UID={};PWD={}'.format(username, password))
    return cnxn

def execute_query(cnxn: pyodbc.Connection, query: str)-> pd.DataFrame:
    with cnxn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        df = pd.DataFrame.from_records(rows, columns=columns)
    return df
