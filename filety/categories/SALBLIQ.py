from typing import Callable, List, Set
import pandas as pd
from collections import OrderedDict
from custom_package import cobol_utils

columns = OrderedDict([('bgcwdh01-saldliq-dt-estraz',  cobol_utils.Column.Text(10)),
('bgcwdh01-saldliq-societa',  cobol_utils.Column.Text(2)),
('bgcwdh01-saldliq-dossier',  cobol_utils.Column.Text(16)),
('bgcwdh01-saldliq-divisa',  cobol_utils.Column.Text(3)),
('bgcwdh01-saldliq-dtrifer',  cobol_utils.Column.Text(10)),
('bgcwdh01-saldliq-tiposaldo',  cobol_utils.Column.Text(1)),
('bgcwdh01-saldliq-cmbpronti', cobol_utils.Column.Comp(6,7)),
('bgcwdh01-saldliq-ctvldiv', cobol_utils.Column.Comp(15,2)),
('bgcwdh01-saldliq-ctvleur', cobol_utils.Column.Comp(15,2)),
('bgcwdh01-saldliq-dareg-eff', cobol_utils.Column.Comp(13,5)),
('bgcwdh01-saldliq-dareg', cobol_utils.Column.Comp(13,5)),
('bgcwdh01-saldliq-liquidita', cobol_utils.Column.Comp(15,2)),
('filler', cobol_utils.Column.Text(104))])

_NK = ['bgcwdh01-saldliq-societa', 'bgcwdh01-saldliq-dossier', 'bgcwdh01-saldliq-divisa',  'bgcwdh01-saldliq-dtrifer','bgcwdh01-saldliq-tiposaldo']

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
    return ebcdic.get_DataFrame(drop_columns=['filler'])

map = {'reading_function': _read,
    'required_columns': set(columns.keys()).difference({'filler'}),
    'processing_function': _process,
    'check_function':_check,
    'natural_key':_NK}