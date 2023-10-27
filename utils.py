from custom_package import filety as ft
from custom_package import data_lake_utils, variables as var, exceptions, azure_function_utils as f_utils
import pandas as pd
import io

def get_current_table(category: str)-> ft.Table:
    '''
    return the processed Table of the given category, reading it from the directory "current2 in the container
    '''
    container = data_lake_utils.get_container(storage_account_name=var.storage_account_name, container_name=var.container_name)
    file_client = container.get_file_client(f'current/{category}.csv')
    if not file_client.exists():
        # return empty Table
        f_utils.warning(f'Returning an empty current table for the category {category}, because no corresponding csv file has been found in the directory "current".')
        columns = ft.FileCategory(category).get_required_columns()
        df = pd.DataFrame(columns=list(columns))
    else:
        csv_bytes = file_client.download_file().readall()
        df = pd.read_csv(io.BytesIO(csv_bytes))
    table = ft.Table(name=category, category=category, df=df)
    return table