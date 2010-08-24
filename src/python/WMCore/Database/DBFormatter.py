"""
_DBFormatter_

A class to define the standardised formatting of database results.
Holds a bunch of helper methods to format input and output of sql
interactions.
"""

__revision__ = "$Id: DBFormatter.py,v 1.6 2008/09/09 12:08:29 metson Exp $"
__version__ = "$Revision: 1.6 $"
import datetime
import time

from WMCore.DataStructs.WMObject import WMObject

class DBFormatter(WMObject):
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
            if type(1L) == type(r):
                print "deal with crappy mysql implementation"
                out.append(r)
            else:
                for i in r.fetchall():
                    out.append(i)
        return out
                
    def formatOne(self, result):
        """
        Return one record
        """
        out = []
        for r in result:
            if r.rowcount == 0:
                return []
            out.append(r.fetchone())
        return out[0]
        
    
    def formatDict(self, result):
        """
        Returns an array of dictionaries representing the results
        """
        out = []
        for r in result:
            description = r.keys
            if r.rowcount == 0:
                return []
            for i in r.fetchall():
                out.append(dict(zip(description, i)))
        return out
  
    def formatOneDict(self, result):
        """
        Return a dictionary representing the results
        """
        out = []
        for r in result:
            description = r.keys
            if r.rowcount == 0:
                return {}
            out.append(dict(zip(description, r.fetchone())))
        return out[0]
    
    def getBinds(self, **kwargs):
        binds = {}
        for i in kwargs.keys():
            binds = self.dbi.buildbinds(self.dbi.makelist(kwargs[i]), i, binds)
        return binds
    
    def execute(self, conn = None, transaction = False):
        """
        A simple select with no binds/arguments is the default
        """
        result = self.dbi.processData(self.sql, self.getBinds(), 
                         conn = conn, transaction = transaction)
        return self.format(result)
    
    def executeOne(self, conn = None, transaction = False):
        """
        A simple select with no binds/arguments is the default
        """
        result = self.dbi.processData(self.sql, self.getBinds(), 
                         conn = conn, transaction = transaction)
        return self.formatOne(result)
