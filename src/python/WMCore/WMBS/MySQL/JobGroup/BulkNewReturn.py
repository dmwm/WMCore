#!/usr/bin/env python
"""
_BulkNewReturn_

MySQL implementation of JobGroup.BulkNewReturn
"""

__all__ = []
__revision__ = "$Id: BulkNewReturn.py,v 1.1 2010/02/25 21:48:17 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class BulkNewReturn(DBFormatter):
    """
    Does a bulk commit of jobGroups, followed by returning their IDs and UIDs

    """
    sql = """INSERT INTO wmbs_jobgroup (subscription, uid, output,
             last_update) VALUES (:subscription, :uid, :output,
             unix_timestamp())"""

    returnSQL = """SELECT ID as ID, UID as UID FROM wmbs_jobgroup
                   WHERE subscription = :subscription
                   AND uid = :uid
                   AND output = :output"""

    def execute(self, bulkInput = None, conn = None, transaction = False):
        """
        This can take a list of dictionaries {subscription, uid, output}
        instead of the original inputs

        """

        binds = []
        for entry in bulkInput:
            binds.append({'subscription': entry['subscription'],
                          'uid': entry['uid'],
                          'output': entry['output']})

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        result = self.dbi.processData(self.returnSQL, binds,
                                      conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
