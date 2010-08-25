#!/usr/bin/env python
"""
_ListForSubmitter_

MySQL function to list jobs for submission
"""

__revision__ = "$Id: ListForSubmitter.py,v 1.1 2010/07/28 15:47:00 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class ListForSubmitter(DBFormatter):
    sql = """SELECT wmbs_job.id AS id, wmbs_job.cache_dir AS cache_dir,
                    wmbs_sub_types.name AS type, wmbs_job.retry_count AS retry_count
                    FROM wmbs_job
               INNER JOIN wmbs_jobgroup ON
                 wmbs_job.jobgroup = wmbs_jobgroup.id
               INNER JOIN wmbs_subscription ON
                 wmbs_jobgroup.subscription = wmbs_subscription.id
               INNER JOIN wmbs_sub_types ON
                 wmbs_subscription.subtype = wmbs_sub_types.id
               INNER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
             WHERE wmbs_job_state.name = 'created'"""

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
