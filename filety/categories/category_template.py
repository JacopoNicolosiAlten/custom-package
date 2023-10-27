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

def _check(df: pd.DataFrame) -> None:
    '''
    Check all required data are included in DMCO file. Raise an exception otherwise
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
    'check_function':_check,
    'natural_key':_NK}