from typing import Callable, List, Set
import pandas as pd
from collections import OrderedDict
from custom_package import cobol_utils

columns = OrderedDict([('bgcwdh01-dmco-dt-estraz', cobol_utils.Column.Text(10)),
    ('bgcwdh01-dmco-operaz', cobol_utils.Column.Text(1)),
    ('bgcwdh01-dmco-societa', cobol_utils.Column.Text(2)),
    ('bgcwdh01-dmco-numreg', cobol_utils.Column.Comp(12)),
    ('bgcwdh01-dmco-progr', cobol_utils.Column.Binary(4)),
    ('bgcwdh01-dmco-kconto', cobol_utils.Column.Text(16)),
    ('bgcwdh01-dmco-impds', cobol_utils.Column.Comp(13,2)),
    ('bgcwdh01-dmco-segno', cobol_utils.Column.Text(1)),
    ('bgcwdh01-dmco-dtval', cobol_utils.Column.Text(10)),
    ('bgcwdh01-dmco-kesterno', cobol_utils.Column.Text(16)),
    ('bgcwdh01-dmco-liq', cobol_utils.Column.Text(1)),
    ('bgcwdh01-dmco-kdiv', cobol_utils.Column.Text(3)),
    ('bgcwdh01-dmco-cmb', cobol_utils.Column.Comp(6,7)),
    ('bgcwdh01-dmco-impdiv', cobol_utils.Column.Comp(13,2)),
    ('bgcwdh01-dmco-filler', cobol_utils.Column.Text(108))])

_NK = ['bgcwdh01-dmco-societa', 'bgcwdh01-dmco-numreg','bgcwdh01-dmco-progr']

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
    Read given bytes as bcedic according to the columns expected in DMCO
    '''
    ebcdic = cobol_utils.Ebcdic(bytes=bytes, columns=columns)
    return ebcdic.get_DataFrame(drop_columns=['bgcwdh01-dmco-filler'])

map = {'reading_function': _read,
    'required_columns': set(columns.keys()).difference({'bgcwdh01-dmco-filler'}),
    'processing_function': _process,
    'check_function':_check,
    'natural_key':_NK}