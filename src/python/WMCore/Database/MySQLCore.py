from WMCore.Database.DBCore import DBInterface
from WMCore.Database.Dialects import MySQLDialect

class MySQLInterface(DBInterface):

    def substitute(self, sql, binds):
        """
        Horrible hacky thing to handle MySQL's lack of bind variable support
        takes a single sql statment and its associated binds
        """
        newsql = sql
        if isinstance(self.engine.dialect, MySQLDialect) and binds is not None:
            for k, v in binds.items():
                newsql = newsql.replace(':%s' % k, "'%s'" % v)
            return newsql, None
        
    def executebinds(self, s=None, b=None, connection=None):
        """
        _executebinds_
        """
        s, b = self.substitute(s, b)
        DBCore.executebinds(s, b, connection)