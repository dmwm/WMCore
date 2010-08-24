#!/usr/bin/env python
"""
_FailFiles_

MySQL implementation of Subscription.FailFiles
"""
__all__ = []
__revision__ = "$Id: FailFiles.py,v 1.1 2008/06/24 15:53:16 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class FailFiles(MySQLBase):
    sql = """insert into wmbs_sub_files_failed 
                (subscription, file) values (:subscription, :file)"""
         
    def execute(self, subscription=None, file=None,
                conn = None, transaction = False):
        
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, file=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)