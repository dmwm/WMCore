#!/usr/bin/env python
"""
_GetGroupsByJobState_

MySQL implementation of JobGroup.GetGroupsByJobState
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetGroupsByJobState(DBFormatter):
    sql = """SELECT DISTINCT wmbs_jobgroup.id FROM wmbs_jobgroup
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
               INNER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
              WHERE wmbs_job_state.name = :job_state
              AND wmbs_jobgroup.subscription >= :minsub"""

    def format(self, results):
        """
        _format_

        Format the jobgroup ids into a single list.
        """
        results = DBFormatter.format(self, results)

        jobGroupList = []
        for result in results:
            jobGroupList.append(result[0])

        return jobGroupList

    def execute(self, jobState = None, minSub = 0, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"job_state": jobState, "minsub": minSub},
                                      conn = conn, transaction = transaction)
        return self.format(result)
