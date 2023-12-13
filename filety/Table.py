from __future__ import annotations
from custom_package import exceptions, dataframe_utils as df_utils, azure_function_utils as f_utils
import pandas as pd
import io
from collections import UserString
from typing import Callable, Set, List, Dict
from custom_package.filety.categories import file_categories_map
from custom_package.filety import DataType as dt

file_categories_set = set(file_categories_map.keys())

def _data_types_check_step(df: pd.DataFrame, data_types: Dict[str, dt.DataType], remediate: bool)-> bool:
    not_consistent_df = ~df_utils.apply_elementwise_to_columns(df, {c: d.is_consistent for (c, d) in data_types.items()})
    not_consistent_columns = not_consistent_df.columns[not_consistent_df.any(axis='index')].tolist()
    for c in not_consistent_columns:
        msg = f'The following values in the column "{c}" are not suitable {data_types[c]}.' + '\n\t' + ('"' + df.loc[not_consistent_df[c], c].drop_duplicates().astype(str) + '"; ').sum()
        f_utils.warning(msg)
    if remediate:
        msg = 'A remediation attempt will be performed:\n\t' + ';\n\t'.join([f'values in "{c}" {data_types[c].remediation_description}' for c in not_consistent_columns]) + '.'
        f_utils.info(msg)
    consistency = not not_consistent_df.any(axis=None)
    return consistency

class FileCategory(UserString):

    def __init__(self, value: str) -> None:
        if value not in file_categories_set:
            raise TypeError('The value {} is not an acceptable file category. Specify one of {}'\
                .format(value, ', '.join(file_categories_set)))
        super().__init__(value)
        self._transformation = file_categories_map[value]['transformation']
        self._pre_check_function = file_categories_map[value]['pre_check_function']
        self._post_check_function = file_categories_map[value]['post_check_function']
        self._columns = file_categories_map[value]['columns']
        self._natural_key = file_categories_map[value]['natural_key']
        self._reading_function = file_categories_map[value]['reading_function']

    def __add__(self, suffix: str) -> str:
        '''
        append a string returns a string
        '''
        return str(self) + suffix
    
    def get_reading_function(self) -> Callable[[bytes], pd.DataFrame]:
        return self._reading_function
    
    def get_transformation(self) -> Callable[[pd.DataFrame], pd.DataFrame]:
        return self._transformation
        
    def get_pre_check_function(self) -> Callable[[pd.DataFrame], None]:
        return self._pre_check_function

    def get_post_check_function(self) -> Callable[[pd.DataFrame], None]:
        return self._post_check_function
    
    def get_data_types(self)-> Dict[str, dt.DataType]:
        return self._columns

    def get_required_columns(self) -> set:
        return set(self._columns.keys())
    
    def get_NK(self) -> List[str]:
        return self._natural_key
    
    def has_period(self) -> bool:
        return 'period' in self.get_required_columns()

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
        return df.to_csv(na_rep='NULL', index=False).encode()
    
    @staticmethod
    def read(name: str, bytes: bytes, category: str)-> Table:
        '''
        return an instance of a Table from the bytes, according to the reading function og the category
        '''
        df = FileCategory(category).get_reading_function()(bytes)
        return Table(name=name, category=category, df=df)
    
    @staticmethod
    def get_empty_table(category: str)-> Table:
        '''
        return an instance of a Table with empty DataFrame
        '''
        df = pd.DataFrame([], columns=list(FileCategory(category).get_required_columns()))
        return Table(name=f'empty-{category}', category=category, df=df)
    
    @staticmethod
    def from_csv_bytes(name: str, category: str, bytes: bytes)-> Table:
        df = pd.read_csv(io.BytesIO(bytes), na_values='NULL', dtype=str)
        return Table(name=name, category=category, df=df)
    
    def set_data_types(self, remediate: bool)-> None:
        df = self.get_DataFrame()
        data_types = self.get_category().get_data_types()
        consistency = _data_types_check_step(df=df, data_types=data_types, remediate=remediate)
        if remediate and not consistency:
            df = df_utils.apply_elementwise_to_columns(df, {c: d.remediate for (c, d) in data_types.items()}).copy()
            consistency = _data_types_check_step(df=df, data_types=data_types, remediate=False)
        if not consistency:
            raise exceptions.DataException('Unable to set the correct data types.')
        df = df_utils.apply_elementwise_to_columns(df, {c: d.convert for (c, d) in data_types.items()}).copy()
        converted_df = df.apply({c: d.set_dtype for (c, d) in data_types.items()}, axis=0).copy()
        self.set_DataFrame(converted_df)
        return
    
    def raise_error(self, exception: Exception)-> None:
        if isinstance(exception, exceptions.DataException):
            exception.message = f'The following error has been raised when processing "{self.get_name()}".\n' + exception.message
            raise exception        
        else:
            raise exception

    def pre_check(self)-> None:
        df = self.get_DataFrame()
        try:
            self.get_category().get_pre_check_function()(df)
        except Exception as e:
            self.raise_error(e)
        return

    def transform(self)-> None:
        df = self.get_DataFrame()
        processed_df = self.get_category().get_transformation()(df)
        self.set_DataFrame(processed_df)
    
    def post_check(self)-> None:
        df = self.get_DataFrame()
        try:
            self.get_category().get_post_check_function()(df)
        except Exception as e:
            self.raise_error(e)
        return

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
        try:
            df_utils.check_multiple_NK(df=df, NK=NK)
        except Exception as e:
            self.raise_error(e)
        return
    
    def process(self, remediate: bool)-> None:
        '''
        Perform all required ops on the table to prepare it
        '''
        try:
            self.select_required_columns()
            self.set_data_types(remediate=remediate)
            self.pre_check()
            self.transform()
            self.check_NK()
            self.post_check()
        except exceptions.DataException as e:
            message = f'I controlli sul file "{self.get_name()}" hanno prodotto il seguente errore.\n'\
                + e.message
            raise exceptions.DataException(message)
        return
    
    def copy(self, name: str=None)-> Table:
        '''
        Return a deep-copy of the Table
        '''
        name = name if name is not None else self.get_name()
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
        self_df = self.get_DataFrame()
        self_name = self.get_name()
        other_df = other.get_DataFrame()
        other_name = other.get_name()

        df = pd.concat([self_df, other_df], axis='index')

        # check for not overlapping
        if len(self_df.drop_duplicates()) + len(other_df.drop_duplicates()) > len(df.drop_duplicates()):
            message = f'The two tables "{self_name}" and "{other_name}" share some rows. This is likely to mean that duplicates would have been uploaded.'
            raise exceptions.DataException(message)
        
        return Table(name = self_name + '+' + other_name, category=self_category, df=df)