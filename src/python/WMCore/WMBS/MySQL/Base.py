"""
Class to define the standardised formatting of MySQL results.
"""
import datetime
import time

class MySQLBase(object):
    def __init__(self, logger, dbinterface):
        self.logger = logger
        self.dbi = dbinterface
    
    def truefalse(self, value):
        if value in ('False', 'FALSE', 'n', 'NO', 'No'):
            value = 0
        return bool(value)
        
    def convertdatetime(self, t):
        return int(time.mktime(t.timetuple()))
          
    def timestamp(self):
        """
        generate a timestamp
        """
        t = datetime.datetime.now()
        return self.convertdatetime(t)
        
    def format(self, result):
        """
        Some standard formatting
        """
        out = []
        for r in result:
            for i in r.fetchall():
                out.append(i)
        return out
    
    def getBinds(self):
        """
        Return the appropriately formatted binds for the sql
        """
        return {}
    
    def execute(self, conn = None, transaction = False):
        """
        A simple select with no binds/arguments is the default
        """
        result = self.dbi.processData(self.sql, self.getBinds(), 
                         conn = conn, transaction = transaction)
        return self.format(result)