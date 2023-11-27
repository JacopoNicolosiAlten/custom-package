import pyodbc
import os
import pandas as pd

def connect_DB(server_name: str, database_name: str) -> pyodbc.Connection:
    connection_string = r'DRIVER={ODBC Driver 17 for SQL Server};' + 'SERVER={};DATABASE={};'.format(server_name, database_name)
    if os.getenv("MSI_SECRET"):
        connection_string += 'Authentication=ActiveDirectoryMsi;'
    else:
        pw = os.getenv('DBpassword')
        connection_string += 'UID=agresso-adm;PWD={}'.format(pw)
    cnxn = pyodbc.connect(connection_string)
    return cnxn

def execute_query(cnxn: pyodbc.Connection, query: str)-> pd.DataFrame:
    with cnxn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        df = pd.DataFrame.from_records(rows, columns=columns)
    return df
