from __future__ import annotations
from collections import UserString, OrderedDict
from typing import Dict, Callable, Union, Any, List, Tuple
import pandas as pd
import numpy as np


type_set = {'Comp', 'Text', 'Binary'}

class Column(UserString):

    def __init__(self, value: str, length: int, decode_function: Callable[[bytes], Any], params: Tuple[int]):
        if value not in type_set:
            raise ValueError(f'"{value}" is not a known Cobol type, specify one among {type_set}')
        super().__init__(value)
        self._length = length
        self.decode = decode_function
        self._params = params

    def get_length(self) -> int:
        return self._length
    
    def get_params(self) -> Tuple[int]:
        return self._params
    
    def __add__(self, other: str) -> str:
        return str(self) + other
    
    def __repr__(self) -> str:
        params = self.get_params()
        return self + '(' + ','.join([str(p) for p in params]) + ')'

    @staticmethod
    def Comp(integer_digits: int, decimal_digits: int = 0) -> Column:
        length = ((integer_digits + decimal_digits + 1) + 1) // 2
        def decode(bytes) -> float:
            if len(bytes) != length:
                raise Exception(f'Attempting to decode {len(bytes)} bytes as Comp({integer_digits}, {decimal_digits}). {length} bytes are required.')
            s = bytes.hex()
            sign = {'c': '+', 'd': '-'}[s[-1].lower()]
            integer_value = s[:-1-decimal_digits]
            decimal_value = s[-1-decimal_digits:-1]
            if decimal_digits == 0:
                return int(sign + integer_value)
            else:
                return float(sign + integer_value + '.' + decimal_value)
        return Column('Comp', length=length, decode_function=decode, params=(integer_digits, decimal_digits))
    
    def Binary(digits: int) -> Column:
        if digits < 5:
            length = 2
        elif digits < 9:
            length = 4
        elif digits < 19:
            length = 8
        else: 
            raise Exception('The maximum number of digits is 18.')
    
        def decode(bytes: bytes) -> int:
            if len(bytes) != length:
                raise Exception(f'Attempting to decode {len(bytes)} bytes as Binary({digits}). {length} bytes are required.')
            s = bytes.hex()
            return int(s)
        return Column('Binary', length=length, decode_function=decode, params=(digits,))
    
    def Text(length: int) -> Column:
        def decode(bytes: bytes, cp='cp500') -> str:
            if len(bytes) != length:
                    raise Exception(f'Attempting to decode {len(bytes)} bytes as Text({length}). {length} bytes are required.')
            return bytes.decode(encoding=cp)
        return Column('Text', length=length, decode_function=decode, params=(length,))

class Ebcdic:
    def __init__(self, bytes: bytes, columns: OrderedDict[str, Column]):
        self.cell_size = {name: c.get_length() for name, c in columns.items()}
        self.row_length = sum(list(self.cell_size.values()))
        if len(bytes) % self.row_length != 0:
            raise Exception(f'The length of the rows for the given columns is {self.row_length}. The number of given bytes {bytes} is not a multiple.')
        self.length = len(bytes) // self.row_length
        self._bytes = bytes
        self.columns = columns

    def __len__(self) -> int:
        return self.length        

    def get_bytes(self) -> bytes:
        return self._bytes
    
    def get_DataFrame(self, drop_columns: List[str] = []) -> pd.DataFrame:
        rows = np.array([self.get_bytes()[i * self.row_length:(i+1)*self.row_length] for i in range(len(self))])
        chunk_points = np.append([0], np.cumsum(list(self.cell_size.values())))
        def split_row(r: bytes) -> np.array[bytes]:
            return np.array([r[chunk_points[i]: chunk_points[i+1]] for i in range(len(chunk_points)-1)], dtype=object)
        rows = np.array([split_row(r) for r in rows], dtype=object)
        df = pd.DataFrame(rows, columns=list(self.columns.keys()), dtype=object).drop(columns=drop_columns)
        for c in df.columns:
            df[c] = df[c].apply(lambda x: self.columns[c].decode(x))
        return df