import pyodbc
import os
import pandas as pd

def connect_DB(server_name: str, database_name: str) -> pyodbc.Connection:
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
            + 'SERVER={};'.format(server_name)
            + 'DATABASE={};'.format(database_name)
            + 'Authentication=ActiveDirectoryMsi;TrustServerCertificate=Yes')
    return cnxn

def execute_query(cnxn, query):
    with cnxn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        df = pd.DataFrame.from_records(rows, columns=columns)
    return df
