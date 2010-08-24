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
                if type(v) == type('string'):
                    newsql = newsql.replace(':%s' % k, "'%s'" % v)
                else:
                    newsql = newsql.replace(':%s' % k, '%s' % v)
        return newsql, None
        
    def executebinds(self, s=None, b=None, connection=None):
        """
        _executebinds_
        """
        s, b = self.substitute(s, b)
        return DBInterface.executebinds(self, s, b, connection)
    
    def executemanybinds(self, s=None, b=None, connection=None):
        """
        _executemanybinds_
        b is a list of dictionaries for the binds, e.g.:
        
        b = [ {'bind1':'value1a', 'bind2': 'value2a'},
        {'bind1':'value1b', 'bind2': 'value2b'} ]
        
        this needs to be reformatted to a list of tuples - TBC! via a cool map 
        function or similar.
        
        b = [ ('value1a', 'value2a'), ('value1b', 'value2b')] 
        
        Don't need to substitute in the binds - looks like executemany does that 
        internally. But the sql will also need to be reformatted, such that 
        :bind_name becomes %s.
        
        See: http://www.devshed.com/c/a/Python/MySQL-Connectivity-With-Python/5/
        """
        newsql = s
        binds = b[0].keys()
        binds.sort(key=s.index)
        for k in binds:
            self.logger.debug("rewriting sql for execute_many: bind %s" % k)
            newsql = newsql.replace(':%s' % k, '%s')

        bind_list = []
        for i in b:
            tpl = tuple( [ i[x] for x in binds] )
            bind_list.append(tpl)

        return DBInterface.executemanybinds(self, newsql, bind_list, connection)