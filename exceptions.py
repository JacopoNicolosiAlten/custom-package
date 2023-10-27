class DataException(Exception):

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

class MissingColumnsException(DataException):

    def __init__(self, missing_columns: set, file_name: str) -> None:
        self.file_name = file_name
        self.missing_columns = missing_columns
        self.message = 'The file {} does not have the following required columns.\n{}'\
            .format(file_name, missing_columns)
        super().__init__(self.message)
