from WMCore.Database.DBCore import DBInterface
from WMCore.Database.Dialects import MySQLDialect

class MySQLInterface(DBInterface):

    def substitute(self, sql, binds):
        """
        Horrible hacky thing to handle MySQL's lack of bind variable support
        takes a single sql statment and its associated binds
        """
        newsql = sql
        if binds and isinstance(self.engine.dialect, MySQLDialect):
            for k, v in binds.items():
                self.logger.debug("substituting bind %s, %s" % (k, v))
                newsql = newsql.replace(':%s' % k, "'%s'" % v)
                
        return newsql, None
        
    def executebinds(self, s=None, b=None, connection=None):
        """
        _executebinds_
        """
        s, b = self.substitute(s, b)
        return DBInterface.executebinds(self, s, b, connection)