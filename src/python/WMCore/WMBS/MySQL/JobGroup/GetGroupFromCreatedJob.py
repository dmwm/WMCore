#!/usr/bin/env python
"""
_GetGroupFromCreatedJob_

MySQL implementation of JobGroup.GetGroupFromCreatedJob
"""

__revision__ = "$Id: GetGroupFromCreatedJob.py,v 1.2 2010/01/15 14:25:22 hufnagel Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetGroupFromCreatedJob(DBFormatter):
    sql = """SELECT DISTINCT wmbs_jobgroup.id FROM wmbs_jobgroup
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
               INNER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
              WHERE wmbs_job_state.name = 'created'"""

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

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {},
                                      conn = conn, transaction = transaction)
        return self.format(result)
