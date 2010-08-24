#!/usr/bin/env python
"""
_Load_

MySQL implementation of LoadFileset

"""
__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.4 2008/11/26 19:46:48 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = """select name, open, last_update from wmbs_fileset 
            where id = :fileset"""
            
    def getBinds(self, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset')
    
    def format(self, result):
        result = result[0].fetchall()[0]
        time = result[2]
        open = self.truefalse(result[1])
        name = result[0]
        return name, open, time
    
    def execute(self, fileset = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(fileset), 
                         conn = conn, transaction = transaction)
        return self.format(result)
