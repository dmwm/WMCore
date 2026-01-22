#!/usr/bin/env python
"""
_GetSubscriptionJobCounts_

MySQL implementation of JobGroup.GetSubscriptionJobCounts
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetSubscriptionJobCounts(DBFormatter):

    sql = """SELECT COUNT(wmbs_job.id),
                    COUNT(wmbs_job_state.id)
             FROM wmbs_subscription
             INNER JOIN wmbs_jobgroup ON
               wmbs_jobgroup.subscription = wmbs_subscription.id
             INNER JOIN wmbs_job ON
               wmbs_job.jobgroup = wmbs_jobgroup.id
             LEFT OUTER JOIN wmbs_job_state ON
               wmbs_job_state.id = wmbs_job.state AND
               wmbs_job_state.name = 'cleanout'
             WHERE wmbs_subscription.id =
               (SELECT subscription FROM wmbs_jobgroup WHERE id = :JOBGROUP)
             """

    def execute(self, jobgroup, conn = None, transaction = False):

        results = self.dbi.processData(self.sql, { "JOBGROUP" : jobgroup },
                                       conn = conn, transaction = transaction)[0].fetchall()

        return (results[0][0], results[0][1])
