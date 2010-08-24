#!/usr/bin/env python
"""
_Load_

MySQL implementation of LoadFileset

"""
__all__ = []
__revision__ = "$Id: LoadSQL.py,v 1.1 2008/06/09 16:30:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Load(MySQLBase):
    sql = """select id, open, last_update from wmbs_fileset 
            where name = :fileset"""
            
    def getBinds(self, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset')
    
    def execute(self, fileset = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(fileset), 
                         conn = conn, transaction = transaction)
        return self.format(result)