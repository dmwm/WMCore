#!/usr/bin/env python
"""
_ExistsByID_

MySQL implementation of Jobs.ExistsByID
"""

__all__ = []
__revision__ = "$Id: ExistsByID.py,v 1.1 2008/12/05 21:02:05 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class ExistsByID(DBFormatter):
    sql = "select id from wmbs_job where id = :id"
    
    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) == 0:
            return False
        else:
            return result[0][0]
    
    def getBinds(self, id):
        return self.dbi.buildbinds(self.dbi.makelist(id), "id")
        
    def execute(self, id):
        result = self.dbi.processData(self.sql, self.getBinds(id))
        return self.format(result)
