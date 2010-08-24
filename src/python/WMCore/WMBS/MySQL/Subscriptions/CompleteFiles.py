#!/usr/bin/env python
"""
_CompleteFiles_

MySQL implementation of Subscription.CompleteFiles
"""
__all__ = []
__revision__ = "$Id: CompleteFiles.py,v 1.1 2008/06/24 15:53:16 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class CompleteFiles(MySQLBase):
    sql = """insert into wmbs_sub_files_complete 
                (subscription, file) values (:subscription, :file)"""
    
    def execute(self, subscription=None, file=None, 
                conn = None, transaction = False):
        
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, file=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)