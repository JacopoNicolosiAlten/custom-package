import pandas as pd
from typing import List, Dict, Tuple
from custom_package import exceptions

def check_multiple_NK(df: pd.DataFrame, NK: List[str]) -> None:
    blank_NK = df[df[NK].isna().any(axis='columns')]
    if len(blank_NK) > 0:
        message = f'There are {len(blank_NK)} rows with a blank value among "'\
            + '", "'.join(NK)\
            + '". This is not allowed because they are used as the key identifier.'
        raise exceptions.DataException(message)
    NK_check = df.groupby(NK).size().rename('Count').reset_index(drop=False)
    NK_check.query('Count > 1', inplace=True)
    if len(NK_check) > 0:
        message = 'The table contains multiple rows for the following tuples "'\
            + '"/"'.join(NK)\
            + '". This is not allowed because they are used as the key identifier.\n'\
            + ('\t' + NK_check[NK].apply(lambda r: '/'.join(r.tolist()), axis='columns') + ': ' + NK_check['Count'].astype(str) + ' rows.\n').sum()
        raise exceptions.DataException(message)
    return


def check_last_row(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Check if the last row of the table is the aggregation, if it is the case, drop it. Then, retrun the result.
    '''
    last_row = df.iloc[-1]
    if last_row.notna().sum() <=1:
        return df.iloc[:-1].copy()
    return df

def split_by_columns(df: pd.DataFrame, columns: List[str]) -> Dict[Tuple[str], pd.DataFrame]:
    '''
    split the dataframe into a dictionary tuple-of-column-values -> dataframe
    according to the values of the specified list of columns
    '''
    res = {(): df}
    for c in columns:
        if c not in df.columns:
            raise KeyError(f'Trying to split a dataframe according to "{c}", which does not match any of its columns.')
        res = {k + (c + ':' + split,): df for k, v in res.items() for split, df in v.groupby(c)}
    return res