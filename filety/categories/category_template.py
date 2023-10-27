'''
This is a template for the category specification scirpt.
The categoty/__init__.py should create a dictionary of all the categories maps, called "files_categories_map"
'''

import pandas as pd

_required_columns: set[str] = {}
_NK: list[str] = []

def _process(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Return the processed dataframe of a table classified as DMCO.
    '''
    return df

def _pre_check(df: pd.DataFrame) -> None:
    '''
    Check all required data are included in file. Raise an exception otherwise.
    Run on the file before any process.
    '''
    return

def _post_check(df: pd.DataFrame)-> None:
    '''
    Check all required data are included in the processed Table. Raise an exception otherwise.
    Run on the file after the process.
    '''
    return

def _read(bytes: bytes) -> pd.DataFrame:
    '''
    Read given bytes
    '''
    return pd.DataFrame({})

map = {'reading_function': _read,
    'required_columns': _required_columns,
    'processing_function': _process,
    'pre_check_function':_pre_check,
    'post_check_function':_post_check,
    'natural_key':_NK}