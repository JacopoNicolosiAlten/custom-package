import pandas as pd
import io, logging
from custom_package import azure_function_utils as f_utils, exceptions

_columns = {'nav date':'str','share code':'str','ta share type':'str','share currency':'str','number of shares':'float','share price':'float','total redemption in share':'float','total subscription in share':'float','total redemption in amount':'float','total subscription in amount':'float','next nav date':'str','previous nav date':'str','net asset value':'float','weight of share':'float','exchange rate with official ccy':'float','official nav':'str','isin qs code':'str'}

_NK = ['isin qs code']

def _process(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Return the processed dataframe of a table classified as DMCO.
    '''
    processed_df = df.astype(_columns)
    processed_df = processed_df[processed_df['official nav'] == 'Y'].copy()
    for c in processed_df.columns:
        if 'date' in c:
            processed_df[c] = pd.to_datetime(processed_df[c], format='%Y%m%d')
    return processed_df

def _check(df: pd.DataFrame) -> None:
    '''
    Check all required data are included in DMCO file. Raise an exception otherwise
    '''
    dates = set(df['nav date'].astype('str'))
    if len(dates) > 1:
        f_utils.warning(f'The table contains more than one date in the column "nav date". The following dates have been found with "official nav" = "Y": {", ".join(dates)}.')

def _read(bytes: bytes) -> pd.DataFrame:
    '''
    Read given bytes as csv according to the columns expected in S1cail
    '''
    df = pd.read_csv(io.BytesIO(bytes), sep='|', dtype=str).rename(columns=str.lower)
    return df

map = {'reading_function': _read,
    'required_columns': set(_columns.keys()),
    'processing_function': _process,
    'check_function':_check,
    'natural_key':_NK}