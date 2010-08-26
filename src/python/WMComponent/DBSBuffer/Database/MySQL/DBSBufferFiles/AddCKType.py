#!/usr/bin/env python

"""
MySQL implementation of AddCKType
"""


__revision__ = "$Id: AddCKType.py,v 1.1 2009/12/02 20:04:23 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class AddCKType(DBFormatter):
    sql = """INSERT INTO dbsbuffer_checksum_type (type)
               VALUES (:cktype)"""
                
    def execute(self, cktype = None, conn = None, transaction = False):

        binds = {'cktype': cktype}

        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return
