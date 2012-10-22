#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Subscription.Exists
"""




from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = """SELECT id FROM wmbs_subscription
             WHERE fileset = :fileset AND workflow = :workflow"""

    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) > 0:
            return result[0][0]
        else:
            return False

    def execute(self, workflow=None, fileset=None, type = None, conn = None,
                transaction = False):
        result = self.dbi.processData(self.sql,
                                      self.getBinds(workflow = workflow,
                                                    fileset = fileset),
                                      conn = conn, transaction = transaction)
        return self.format(result)
