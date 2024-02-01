import os
from azure.storage import filedatalake as dl
from azure.identity import ManagedIdentityCredential, InteractiveBrowserCredential
from custom_package import azure_function_utils as f_utils, dataframe_utils as df_utils
import datetime as dt
from pytz import timezone
from typing import List
import pandas as pd

def get_container(storage_account_name: str, container_name: str) -> dl.FileSystemClient:
    '''
    Get the file system client given its name and its storage account
    '''
    if os.getenv('MSI_SECRET'):
        key = ManagedIdentityCredential()
        connection_verify=True
    else:
        # prompt for interactive login authentication if run on localhost
        key = InteractiveBrowserCredential()
        connection_verify=False
    return dl.DataLakeServiceClient(account_url="https://{}.dfs.core.windows.net"\
            .format(storage_account_name), credential=key, connection_verify=connection_verify)\
        .get_file_system_client(file_system=container_name)

def clear_directory(file_system_client: dl.FileSystemClient, dir: str) -> None:
    '''
    Delete all the blobs from the specified directory
    '''
    paths = file_system_client.get_paths(path=dir + '/', recursive=True)
    for path in paths:
        file_system_client.get_file_client(path.name).delete_file()
    f_utils.info(f'The directory "{dir}" has been cleaned.')
    return

def collect_file_bytes(file_system_client: dl.FileSystemClient, path: str) -> dict[str, bytes]:
    '''
    Return a dictionary {file_name -> bytes} for all the files that matches the specified starting path.
    '''
    result = dict()
    paths = file_system_client.get_paths(path=path, recursive=True)
    for path in paths:
        path = path.name
        file_name = path.split('/')[-1]
        # copy the file
        file_client = file_system_client.get_file_client(path)
        file_bytes = file_client.download_file().readall()
        result[file_name] = file_bytes
    return result

def delete_files(file_system_client: dl.FileSystemClient, path: str) -> None:
    '''
    Delete all the files that matches the specified starting path.
    '''
    paths = file_system_client.get_paths(path)
    for p in paths:
        file_system_client.delete_file(p.name)
    f_utils.info('"{}" has been cleaned.'.format(path.strip('/')))
    return

def delete_directory(file_system_client: dl.FileSystemClient, dir: str)-> None:
    dir_client = file_system_client.get_directory_client(dir)
    dir_client.delete_directory()

def backup_files(file_system_client: dl.FileSystemClient, path: str, sink_dir: str, hierarchy: str='Ymd') -> None:
    '''
    Create the directory YYYY-mm-dd (according to hierarchy) in the specified sink_dir, and perform a back-up of the files that match the prefix path, appending the timestamp to the name of each file.
    in hierarchy, include 'Y' if you want to add a year-level parent; 'm' for the month-level parent; 'd' for the day-level parent.
    '''
    timestamp = dt.datetime.now(timezone('Europe/Rome'))
    sink_path = sink_dir
    if 'Y' in hierarchy:
        sink_path = os.path.join(sink_path, timestamp.strftime('%Y'))
    if 'm' in hierarchy:
        sink_path = os.path.join(sink_path, timestamp.strftime('%Y-%m'))
    if 'd' in hierarchy:
        sink_path = os.path.join(sink_path, timestamp.strftime('%Y-%m-%d'))
    datetime_string = timestamp.strftime('%Y-%m-%dT%H:%M:%S')
    # copy-paste the content of the input folder in the day directory
    file_bytes_dict = collect_file_bytes(file_system_client=file_system_client, path=path)
    for full_name, file_bytes in file_bytes_dict.items():
        file_name, extension = os.path.splitext(full_name)
        period_file_client = file_system_client.create_file(os.path.join(sink_path, file_name + '-' + datetime_string + extension))
        period_file_client.upload_data(file_bytes, overwrite=True)
    f_utils.info(f'Input raw files have been pasted into the directory "{sink_path}".')
    return

def backup_splitted_df(file_system_client: dl.FileSystemClient, columns: List[str], df: pd.DataFrame, dir: str, file_name: str) -> None:
    '''
    split the dataframe by the specified columns and save the result as csv's in the specified dir, in the path built according to columns
    '''    
    timestamp = dt.datetime.now(timezone('Europe/Rome')).strftime('%Y-%m-%dT%H:%M:%S')
    sink_directory_client = file_system_client.get_directory_client(dir)
    if not sink_directory_client.exists():
        sink_directory_client.create_directory()
    split_dict = df_utils.split_by_columns(df, columns)
    for k, v in split_dict.items():
        base_name = '/'.join(k) + '/' + file_name
        file_client = sink_directory_client.get_file_client(f'{base_name}-{timestamp}.csv')
        file_client.create_file()
        file_client.upload_data(v.to_csv(na_rep='NULL', index=False).encode(), overwrite=True)
    return