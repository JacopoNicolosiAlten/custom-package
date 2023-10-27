from typing import Callable, List, Set
import pandas as pd
from collections import OrderedDict
from custom_package import cobol_utils

columns = OrderedDict([('bgcwdh01-tmco-dt-estraz', cobol_utils.Column.Text(10)),
    ('bgcwdh01-tmco-operaz', cobol_utils.Column.Text(1)),
    ('bgcwdh01-tmco-societa', cobol_utils.Column.Text(2)),
    ('bgcwdh01-tmco-numreg', cobol_utils.Column.Comp(12)),
    ('bgcwdh01-tmco-kcaup', cobol_utils.Column.Text(3)),
    ('bgcwdh01-tmco-dtreg', cobol_utils.Column.Text(10)),
    ('bgcwdh01-tmco-dtreg-iv', cobol_utils.Column.Text(1)),
    ('bgcwdh01-tmco-dtop', cobol_utils.Column.Text(10)),
    ('bgcwdh01-tmco-dtop-iv', cobol_utils.Column.Text(1)),
    ('bgcwdh01-tmco-dtcont', cobol_utils.Column.Text(10)),
    ('bgcwdh01-tmco-dtcont-iv', cobol_utils.Column.Text(1)),
    ('bgcwdh01-tmco-fonte', cobol_utils.Column.Text(3)),
    ('bgcwdh01-tmco-numsmit2', cobol_utils.Column.Comp(12)),
    ('bgcwdh01-tmco-f02', cobol_utils.Column.Text(1)),
    ('bgcwdh01-tmco-stato', cobol_utils.Column.Text(1)),
    ('bgcwdh01-tmco-dtstorno', cobol_utils.Column.Text(10)),
    ('bgcwdh01-tmco-dtstorno-iv', cobol_utils.Column.Text(1)),
    ('bgcwdh01-tmco-numstorno', cobol_utils.Column.Comp(12))])

_NK = ['bgcwdh01-tmco-societa', 'bgcwdh01-tmco-numreg']

def _process(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Return the processed dataframe of a table classified as TMCO.
    '''
    return df

def _check(df: pd.DataFrame) -> None:
    '''
    Check all required data are included in TMCO file. Raise an exception otherwise
    '''
    return

def _read(bytes: bytes) -> pd.DataFrame:
    '''
    Read given bytes as bcedic according to the columns expected in TMCO
    '''
    ebcdic = cobol_utils.Ebcdic(bytes=bytes, columns=columns)
    return ebcdic.get_DataFrame()

map = {'reading_function': _read,
    'required_columns': set(columns.keys()),
    'processing_function': _process,
    'check_function':_check,
    'natural_key':_NK}