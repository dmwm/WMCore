#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.AcquireFiles
"""
__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.5 2008/11/24 21:46:59 sryu Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class AcquireFiles(DBFormatter):
    sql = """insert into wmbs_sub_files_acquired 
                (subscription, file) values (:subscription, :fileid)"""
    
    def format(self, result):
        return True
        
    def execute(self, subscription=None, file=None,
                conn = None, transaction = False):
        file = self.makelist(file)
        if len(file) == 0:
            self.logger.warning('No files acquired for subscription id %s' % subscription)
            return 0
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, fileid=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)

