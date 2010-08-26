#!/usr/bin/env python
"""
_ExistsForAccountant_

MySQL implementation of Files.ExistsForAccountant
"""

__all__ = []
__revision__ = "$Id: ExistsForAccountant.py,v 1.1 2010/05/24 20:35:27 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class ExistsForAccountant(DBFormatter):
    """
    This is highly specialized.  You shouldn't confuse it with
    a normal Exists DAO


    """
    sql = "SELECT lfn FROM dbsbuffer_file WHERE lfn = :lfn"
    

    
    def getBinds(self, lfn=None):
        return self.dbi.buildbinds(self.dbi.makelist(lfn), "lfn")
        
    def execute(self, lfn=None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(lfn), 
                         conn = conn, transaction = transaction)
        return self.format(result)
