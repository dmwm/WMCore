#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.AcquireFiles
"""
__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.3 2008/10/30 11:06:28 jcgon Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class AcquireFiles(MySQLBase):
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

