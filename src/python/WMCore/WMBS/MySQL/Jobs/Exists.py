#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Jobs.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2008/11/20 21:52:32 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = "select id from wmbs_job where name = :name"
    
    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) == 0:
            return False
        else:
            return result[0][0]
    
    def getBinds(self, name):
        return self.dbi.buildbinds(self.dbi.makelist(name), "name")
        
    def execute(self, name):
        result = self.dbi.processData(self.sql, self.getBinds(name))
        return self.format(result)
