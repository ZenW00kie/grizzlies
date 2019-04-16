from .sql import SQLDataFrame
from .csv import CSVDataFrame

class DataFrame(object):

    def __init__(self, type=None, **kwargs):
        if type == 'sql':
            self.__frame = SQLDataFrame(**kwargs)
        elif type == 'csv':
            self.__frame = CSVDataFrame(**kwargs)

    @classmethod
    def from_sql(cls, engine, schema=None, table=None, query=None):
        return cls(type='sql', engine=engine, schema=None, table=table, query=query)

    @classmethod
    def from_csv(cls, path):
        # return cls(type='csv')
        raise NotImplementedError('This feature is forthcoming.')

    def count(self):
        return self.__frame.count()

    @property
    def columns(self):
        return self.__frame.columns

    def head(self, n=5):
        return self.__frame.head(n)

    def tail(self, n=5):
        return self.__frame.tail(n)
