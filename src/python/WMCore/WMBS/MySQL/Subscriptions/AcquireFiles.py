#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.AcquireFiles
"""
__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.4 2008/11/20 21:52:32 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class AcquireFiles(DBFormatter):
    sql = """insert into wmbs_sub_files_acquired 
                (subscription, file) values (:subscription, :file)"""
        
    def execute(self, subscription=None, file=None,
                conn = None, transaction = False):
        file = self.makelist(file)
        if len(file) == 0:
            self.logger.warning('No files acquired for subscription id %s' % subscription)
            return 0
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, file=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)

