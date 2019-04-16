import pandas as pd

class SQLDataFrame(object):

        def __init__(self, engine, schema=None, table=None, query=None):
            if schema and not table:
                raise ValueError('Specify table name')
            self.__conn = engine.connect()
            self.__table = table
            self.__query = query
            self.__schema = schema

        @property
        def columns(self):
            query = 'select * from %s limit 0' % self._query()
            return self.__conn.execute(query).keys()

        def count(self):
            query = 'select count(*) from %s' % self._query()
            return self.__conn.execute(query).fetchone()[0]

        def head(self, n):
            query = 'select * from %s limit %d' % (self._query(), n)
            result = self.__conn.execute(query)
            return pd.DataFrame(result.fetchall(), columns=result.keys())

        def tail(self, n):
            query = '''select *
                    from %s
                    limit %d
                    offset (select count(*) from %s) - %d
                    ''' % (self._query(), n, self._query(), n)
            result = self.__conn.execute(query)
            return pd.DataFrame(result.fetchall(), columns=result.keys())

        def describe(self):
            return None

        def to_pandas(self, **kwargs):
            if self.__query:
                sql = self.__query
            elif self.__schema:
                sql = self.__schema + '.' + self.__table
            elif self.__table:
                sql = self.__table
            return pd.read_sql(sql, engine, **kwargs)

        def _query(self):
            if self.__schema:
                return '%s.%s' % (self.__schema, self.__table)
            elif self.__table:
                return self.__table
            elif self.query:
                return '(%s) a' % self.__query
