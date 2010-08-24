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
        
        Seems the sqlalchemy's executemany on MySQL is a bit flakey, so instead 
        we get the MySQLDB connection, make a cursor and interact with that 
        directly. 
        """
        
        b = self.makelist(b)
        try: 
            newsql = s
            binds = b[0].keys()
            binds.sort(key=s.index)
            self.logger.debug("MySQLCore.executemanybinds: rewriting sql for execute_many: sql %s" % 
                              s.replace('\n', ' '))
            for k in binds:
                newsql = newsql.replace(':%s' % k, '%s')
    
            bind_list = []
            for i in b:
                tpl = tuple( [ i[x] for x in binds] )
                bind_list.append(tpl)
                
            self.logger.debug("MySQLCore.executemanybinds: rewritten sql for execute_many: sql %s" % \
                              newsql.replace('\n', ' '))
            self.logger.debug("MySQLCore.executemanybinds: rewritten binds: %s" % bind_list)
        except Exception, e:
            self.logger.exception("""MySQLCore.executemanybinds failed - sql : %s
binds : %s
exception : %s""" % (s, b, e.message))
            raise e
        cur = connection.connection.cursor()
        
        result = cur.executemany(newsql, bind_list)
        result = self.makelist(result)
        cur.close()
        return result
        #return DBInterface.executemanybinds(self, newsql, bind_list, connection)