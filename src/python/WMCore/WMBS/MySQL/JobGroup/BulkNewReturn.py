#!/usr/bin/env python
"""
_BulkNewReturn_

MySQL implementation of JobGroup.BulkNewReturn
"""




import time

from WMCore.Database.DBFormatter import DBFormatter

class BulkNewReturn(DBFormatter):
    """
    Does a bulk commit of jobGroups, followed by returning their IDs and UIDs

    """
    sql = """INSERT INTO wmbs_jobgroup (subscription, guid, output,
             last_update) VALUES (:subscription, :guid, :output,
             :timestamp)"""

    returnSQL = """SELECT id AS id, guid AS guid FROM wmbs_jobgroup
                   WHERE subscription = :subscription
                   AND guid = :guid
                   AND output = :output"""

    def execute(self, bulkInput = None, conn = None, transaction = False):
        """
        This can take a list of dictionaries {subscription, guid, output}
        instead of the original inputs

        """
        timestamp = int(time.time())
        insertBinds = []
        returnBinds = []
        for entry in bulkInput:
            insertBinds.append({'subscription': entry['subscription'],
                                'guid': entry['uid'],
                                'output': entry['output'],
                                'timestamp': timestamp})
            returnBinds.append({'subscription': entry['subscription'],
                                'guid': entry['uid'],
                                'output': entry['output']})


        self.dbi.processData(self.sql, insertBinds, conn = conn,
                             transaction = transaction)
        result = self.dbi.processData(self.returnSQL, returnBinds,
                                      conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
