#!/usr/bin/env python
"""
_GetFeederId_

MySQL implementation of FeederManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetFeederId(DBFormatter):

    sql = """
SELECT id from managed_feeders WHERE feeder_type = :type"""

    def getBinds(self, feederType = ''):
        """
        Bind parameters
        """
        dict = {'type': feederType}
        return dict

    def execute(self, feederType = '', conn = None, transaction = False):
        """
        Get the feeder id given the type
        """
        binds = self.getBinds(feederType)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return self.formatOne(result)[0]
