#!/usr/bin/env python
"""
_GetWorkflowInjected_

MySQL implementation of JobGroup.GetWorkflowInjected
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetWorkflowInjected(DBFormatter):

    sql = """SELECT injected
             FROM wmbs_workflow
             WHERE id = (SELECT workflow FROM wmbs_subscription
                         WHERE id = (SELECT subscription FROM wmbs_jobgroup
                                     WHERE id = :JOBGROUP))
             """

    def execute(self, jobgroup, conn = None, transaction = False):

        results = self.dbi.processData(self.sql, { "JOBGROUP" : jobgroup },
                                       conn = conn, transaction = transaction)[0].fetchall()

        return bool(results[0][0])
