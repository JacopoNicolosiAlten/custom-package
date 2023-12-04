from __future__ import annotations
import numbers
from typing import List, Sequence, Any, Dict, Union
import re
from typing_extensions import SupportsIndex
import pandas as pd
import numpy as np
from pandas._typing import Dtype, Scalar, ArrayLike
from pandas.api.extensions import ExtensionDtype as ExtensionDtype, register_extension_dtype, ExtensionDtype, ExtensionArray
from pandas.core.arrays import ExtensionArray
from collections import UserString
from abc import ABC, abstractmethod
import logging



class DataType(ABC):

    @abstractmethod
    def __str__(self)-> str:
        pass

    @property
    @abstractmethod
    def dtype(self)-> Dtype:
        pass

    @property
    @abstractmethod
    def na(self)-> Any:
        pass

    @property
    @abstractmethod
    def name(self)-> str:
        pass

    @abstractmethod
    def is_consistent(self, value: Scalar)-> bool:
        pass

    def validate(self, value: Scalar)-> None:
        if not self.is_consistent(value):
            raise ValueError(f'{value} is not a valid {self}.')
        return

    @abstractmethod
    def remediate(self, value: Scalar)-> Scalar:
        pass

class Varchar(DataType):

    def __init__(self, max_length: int)-> None:
        if not isinstance(max_length, int):
            raise TypeError(f'max_length must be a int. Got {type(max_length)}.')
        self._max_length = max_length

    def __str__(self)-> str:
        return f'varchar[{self._max_length}]'
    
    @property
    def name(self)-> str:
        str(self)
    
    @property
    def dtype(self)-> Dtype:
        return pd.StringDtype()
    
    @property
    def na(self):
        return pd.NA
    
    def is_consistent(self, value: Scalar) -> bool:
        return pd.isna(value) or (isinstance(value, str) and len(value) <= self._max_length)
    
    def remediate(self, value: Scalar) -> Scalar:
        if pd.isna(value):
            return pd.NA
        try:
            value = str(value)
        except:
            return value
        return value[:min(len(value), self._max_length)]
    
class Decimal(DataType):

    def __init__(self, decimal_digits: int, comma_separated: bool = False)-> None:
        if not isinstance(decimal_digits, int):
            raise TypeError(f'max_length must be a int. Got {type(decimal_digits)}.')
        if not isinstance(comma_separated, bool):
            raise TypeError(f'comma_separated must be a boolean. Got {type(comma_separated)}.')
        self._comma_separated = comma_separated
        self._decimal_digits = decimal_digits

    def __str__(self)-> str:
        string = f'decimal[{self._decimal_digits}]'
        if self._comma_separated:
            string = 'comma-separated ' + string
        return string
    
    @property
    def name(self)-> str:
        str(self)
    
    @property
    def dtype(self)-> Dtype:
        return np.dtype('float64')
    
    @property
    def na(self):
        return pd.NA
    
    def format_string(self, string: str)-> str:
        if not self._comma_separated:
            return string.replace(',', '')
        else:
            return string.replace('.', '').replace(',', '.')
    
    def is_consistent(self, value: Scalar) -> bool:
        if pd.isna(value):
            return True
        try:
            value = str(value)
            value = self.format_string(value)
            float(value)
        except:
            return False
        return value.rstrip('0')[::-1].find('.') <= self._decimal_digits
    
    def remediate(self, value: Scalar) -> Scalar:
        if pd.isna(value):
            return pd.NA
        try:
            value = str(value)
            value = self.format_string(value)
            float(value)
        except:
            return value
        return str(round(float(value), 2))