import pandas as pd
import io
from custom_package import azure_function_utils as f_utils, exceptions

_columns = {'commento del gestore', 'data del commento', 'fondo', 'gestore del fondo'}

def _process(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Return the processed dataframe of a table classified as DMCO.
    '''
    processed_df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(processed_df['data del commento']):
        processed_df['data del commento'] = pd.to_datetime(processed_df['data del commento'], format='%d/%m/%Y')
    processed_df['data del commento'] = processed_df['data del commento'].map(lambda x: x.date())
    return processed_df

def _check(df: pd.DataFrame) -> None:
    '''
    Check all required data are included in DMCO file. Raise an exception otherwise
    '''
    length = len(df['commento del gestore'].iloc[0])
    if length > 2600:
        raise exceptions.DataException(f'Il commento supera i 2600 caratteri massimi. Ne contiente {length}.')

def _read(bytes: bytes) -> pd.DataFrame:
    '''
    Read given bytes as csv according to the columns expected in S1cail
    '''
    df = pd.read_excel(io.BytesIO(bytes), header=None, index_col=0).rename(index=lambda c: c.lower().strip(' '))
    return df.iloc[:,[0]].transpose()

map = {'reading_function': _read,
    'required_columns': _columns,
    'processing_function': _process,
    'check_function': _check,
    'natural_key': []}