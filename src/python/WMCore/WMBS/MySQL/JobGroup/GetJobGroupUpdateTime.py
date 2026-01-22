#!/usr/bin/env python
"""
_GetJobGroupUpdateTime_

MySQL implementation of JobGroup.GetSubscriptionJobCounts
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetJobGroupUpdateTime(DBFormatter):

    sql = """SELECT last_update
             FROM wmbs_jobgroup
             WHERE id = :JOBGROUP
             """

    def execute(self, jobgroup, conn = None, transaction = False):

        results = self.dbi.processData(self.sql, { "JOBGROUP" : jobgroup },
                                       conn = conn, transaction = transaction)[0].fetchall()

        return results[0][0]
