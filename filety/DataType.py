from __future__ import annotations
import datetime
from typing import List, Sequence, Any, Dict, Union, Mapping
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
    def na(self):
        pass

    @abstractmethod
    def _convert(self, value: str)-> str:
        pass

    def convert(self, value: str)-> str:
        if pd.isna(value):
            return self.na
        value = str(value)
        return self._convert(value)

    @property
    def na(self)-> Any:
        return self.dtype.na_value

    @property
    @abstractmethod
    def name(self)-> str:
        pass

    @abstractmethod
    def _check_constraint(self, str)-> bool:
        pass

    def is_consistent(self, value: Scalar)-> bool:
        try:
            value = self.convert(value)
        except (ValueError, TypeError):
            return False
        if value is self.na:
            return True
        elif not isinstance(value, str):
            return False
        else:
            return self._check_constraint(value)

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
    
    def _convert(self, value: str) -> str:
        return value
    
    def _check_constraint(self, value: str) -> bool:
        return len(value) <= self._max_length
    
    def remediate(self, value: Scalar) -> Scalar:
        if pd.isna(value):
            return pd.NA
        try:
            value = str(value)
        except:
            return value
        value = re.sub(pattern='\s', repl=' ', string=value).strip(' ')
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
        return np.nan
    
    def _convert(self, value: str) -> str:
        value = self.format_string(value)
        value = float(value)
        return str(value)
    
    def format_string(self, string: str)-> str:
        if not self._comma_separated:
            return string.replace(',', '')
        else:
            return string.replace('.', '').replace(',', '.')
    
    def _check_constraint(self, value: str) -> bool:
        return value.rstrip('0')[::-1].find('.') <= self._decimal_digits
    
    def remediate(self, value: Scalar) -> Scalar:
        if pd.isna(value):
            return self.na
        try:
            value = str(value)
            value = self.format_string(value)
            float(value)
        except:
            return value
        value = str(round(float(value), 2))
        if self._comma_separated:
            value = value.replace('.', ',')
        return value

class Float(Decimal):

    def __init__(self, comma_separated: bool = False) -> None:
        super().__init__(decimal_digits=53, comma_separated=comma_separated)

class Date(DataType):

    def __init__(self, format='%Y-%m-%d') -> None:
        if not isinstance(format, str):
            raise TypeError(f'max_length must be a int. Got {type(format)}.')
        self._format = format

    def __str__(self)-> str:
        return f'date[{self._format}]'
    
    @property
    def name(self)-> str:
        str(self)
    
    @property
    def dtype(self)-> Dtype:
        return np.dtype('datetime64[D]')
    
    @property
    def na(self):
        return pd.NaT
    
    def _convert(self, value: str) -> str:
        length = len(datetime.date(1,1,1).strftime(self._format))
        value = value[:length]
        value = datetime.datetime.strptime(value, self._format)
        return value.strftime('%Y-%m-%d')
    
    def _check_constraint(self, value: str) -> bool:
        return True
    
    def remediate(self, value: Scalar) -> Scalar:
        if pd.isna(value):
            return self.na
        try:
            value = str(value)
            try:
                value = pd.to_datetime(value, format=self._format, infer_datetime_format=False).strftime(self._format)
            except:
                value = pd.to_datetime(value, infer_datetime_format=True).strftime(self._format)
        except:
            return value
        return value