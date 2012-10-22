#!/usr/bin/env python
"""
_IDFromFilesetWorkflow_

MySQL implementation of Subscription.IDFromFilesetWorkflow
"""




from WMCore.Database.DBFormatter import DBFormatter

class IDFromFilesetWorkflow(DBFormatter):
    sql = """SELECT id FROM wmbs_subscription
             WHERE fileset = (SELECT id FROM wmbs_fileset WHERE name = :fileset)
             AND workflow = (SELECT id FROM wmbs_workflow WHERE name = :workflow)
             """
    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) > 0:
            return result[0][0]
        else:
            return False

    def execute(self, workflow = None, fileset = None, conn = None,
                transaction = False):
        result = self.dbi.processData(self.sql, {"workflow": workflow, "fileset": fileset},
                                      conn = conn, transaction = transaction)
        return self.format(result)
