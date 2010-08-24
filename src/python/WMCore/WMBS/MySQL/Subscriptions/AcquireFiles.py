#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.AcquireFiles
"""
__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.2 2008/10/28 18:48:10 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class AcquireFiles(MySQLBase):
    sql = """insert into wmbs_sub_files_acquired 
                (subscription, file) values (:subscription, :file)"""
        
    def execute(self, subscription=None, file=None,
                conn = None, transaction = False):
        if len(file) == 0:
            self.logger.warning('No files acquired for subscription id %s' % subscription)
            return 0
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, file=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)

