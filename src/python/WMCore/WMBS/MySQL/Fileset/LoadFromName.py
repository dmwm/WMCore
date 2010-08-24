#!/usr/bin/env python
"""
_Load_

MySQL implementation of LoadFileset

"""
__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.2 2008/07/03 09:55:02 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class LoadFromName(MySQLBase):
    sql = """select id, open, last_update from wmbs_fileset 
            where name = :fileset"""
            
    def getBinds(self, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset')
    
    def format(self, result):
        result = result[0].fetchall()[0]
        time = self.convertdatetime(result[2])
        open = self.truefalse(result[1])
        id = int(result[0])
        return id, open, time
    
    def execute(self, fileset = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(fileset), 
                         conn = conn, transaction = transaction)
        return self.format(result)