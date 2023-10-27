from __future__ import annotations
import exceptions, dataframe_utils as df_utils
from custom_package.filety.categories import file_categories_map
import pandas as pd
from collections import UserString, OrderedDict
from typing import Callable, Set, List, Union
import io

file_categories_set = set(file_categories_map.keys())# file_categories_map.keys())

class FileCategory(UserString):

    def __init__(self, value: str) -> None:
        if value not in file_categories_set:
            raise TypeError('The value {} is not an acceptable file category. Specify one of {}'\
                .format(value, ', '.join(file_categories_set)))
        super().__init__(value)
        self._processing_function = file_categories_map[value]['processing_function']
        self._check_function = file_categories_map[value]['check_function']
        self._required_columns = file_categories_map[value]['required_columns']
        self._natural_key = file_categories_map[value]['natural_key']
        self._reading_function = file_categories_map[value]['reading_function']

    def __add__(self, suffix: str) -> str:
        '''
        append a string returns a string
        '''
        return str(self) + suffix
    
    def get_reading_function(self) -> Callable[[bytes], pd.DataFrame]:
        return self._reading_function
    
    def get_processing_function(self) -> Callable[[pd.DataFrame], pd.DataFrame]:
        return self._processing_function
        
    def get_check_function(self) -> Callable[[pd.DataFrame], None]:
        return self._check_function

    def get_required_columns(self) -> set:
        return self._required_columns
    
    def get_NK(self) -> List[str]:
        return self._natural_key
    
    def has_period(self) -> bool:
        return 'Period' in self.get_required_columns()

class Table:

    def __init__(self, name: str, category: str, df: pd.DataFrame):
        self._category = FileCategory(category)
        self._df = df
        self._name = name
    
    def set_DataFrame(self, df: pd.DataFrame)-> None:
        self._df = df

    def set_category(self, category: str)-> None:
        self._category=FileCategory(category)

    def rename(self, name: str)-> None:
        self._name = name

    def get_DataFrame(self)-> pd.DataFrame:
        return self._df.copy()
    
    def get_category(self)-> FileCategory:
        return self._category
    
    def get_name(self) -> str:
        return self._name
    
    def get_columns(self) -> Set[str]:
        return set(self._DataFrame.columns)
    
    def get_csv_bytes(self)-> bytes:
        '''
        return csv bytes of the Table
        '''
        df = self.get_DataFrame()
        return df.to_csv(na_rep='', index=False).encode()
    
    @staticmethod
    def read(name: str, bytes: bytes, category: str)-> Table:
        '''
        return an instance of a Table from the bytes, according to the reading function og the category
        '''
        df = FileCategory(category).get_reading_function()(bytes)
        return Table(name=name, category=category, df=df)

    def process(self)-> None:
        df = self.get_DataFrame()
        processed_df = self.get_category().get_processing_function()(df)
        self.set_DataFrame(processed_df)
    
    def check(self)-> None:
        df = self.get_DataFrame()
        self.get_category().get_check_function()(df)

    def select_columns(self, columns: Set[str]) -> None:
        '''
        Select only the specified columns from the table.
        '''
        df = self.get_DataFrame()
        missing_columns = columns.difference(df.columns)
        if len(missing_columns) > 0:
            raise exceptions.MissingColumnsException(file_name=self.get_name(), missing_columns=missing_columns)
        self.set_DataFrame(df[list(columns)])
        return

    def select_required_columns(self)-> None:
        '''
        Select the required columns, according to the file category
        '''
        category = self.get_category()
        required_columns = category.get_required_columns()
        self.select_columns(required_columns)
        return
    
    def check_NK(self)-> None:
        '''
        Check the natural key constraint on the table
        '''
        df = self.get_DataFrame()
        NK = self.get_category().get_NK()
        if len(NK) == 0:
            return
        df_utils.check_multiple_NK(df=df, NK=NK)
        return
    
    def process_check(self)-> None:
        '''
        Perform all required ops on the table to prepare it
        '''
        try:
            self.select_required_columns()
            self.process()
            self.check_NK()
            self.check()
        except exceptions.DataException as e:
            message = f'I controlli sul file "{self.get_name()}" hanno prodotto il seguente errore.\n'\
                + e.message
            raise exceptions.DataException(message)
        return
    
    def copy(self, name: str)-> Table:
        '''
        Return a deep-copy of the Table
        '''
        df = self.get_DataFrame()
        category = self.get_category()
        return Table(name=name, category=category, df=df)
    
    def __add__(self, other: Table)-> Table:
        '''
        sum as the row concatenation of two tables with the same category
        '''
        self_category = self.get_category()
        other_category = other.get_category()
        if self_category != other_category:
            raise Exception(f'Trying to add two tables with different categories: {self_category}, {other_category}.')
        df = pd.concat([self.get_DataFrame(), other.get_DataFrame()], axis='index').drop_duplicates()
        return Table(name = self.get_name() + '+' + other.get_name(), category=self_category, df=df)