#!/usr/bin/env python
"""
_GetJobGroups_

MySQL implementation of Subscriptions.GetJobGroups
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetJobGroups(DBFormatter):
    sql = """SELECT DISTINCT wmbs_jobgroup.id FROM wmbs_jobgroup
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
               INNER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
              WHERE wmbs_jobgroup.subscription = :subscription AND
                    wmbs_job_state.name = 'new'"""

    def format(self, results):
        """
        _format_

        Format the results into a single list of job group IDs.
        """
        results = DBFormatter.format(self, results)

        jobGroupIDs = []
        for result in results:
            for row in result:
                jobGroupIDs.append(int(row))

        return jobGroupIDs

    def execute(self, subscription = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"subscription": subscription},
                                      conn = conn, transaction = transaction)
        return self.format(result)
