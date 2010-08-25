#!/usr/bin/env python
"""
_Status_

MySQL implementation of Workflow.Status
"""

__revision__ = "$Id: Status.py,v 1.3 2010/05/27 20:20:18 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class Status(DBFormatter):
    """
    _Status_

    """
    sql = """SELECT wmbs_workflow.owner, wmbs_workflow.task, wmbs_job_state.name,
                    COUNT(wmbs_job.id) AS jobs, SUM(wmbs_job.outcome) AS success,
                    SUM(wmbs_fileset.open) AS open FROM wmbs_workflow
               INNER JOIN wmbs_subscription ON
                 wmbs_workflow.id = wmbs_subscription.workflow
               INNER JOIN wmbs_fileset ON
                 wmbs_subscription.fileset = wmbs_fileset.id
               LEFT OUTER JOIN wmbs_jobgroup ON
                 wmbs_subscription.id = wmbs_jobgroup.subscription
               LEFT OUTER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
               LEFT OUTER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
               GROUP BY wmbs_workflow.owner, wmbs_workflow.task, wmbs_job_state.name"""
        
    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        return self.formatDict(result)
