#!/usr/bin/env python
"""
_GetFWJRTaskName_

MySQL implementation of Jobs.GetFWJRTaskName
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetFWJRTaskName(DBFormatter):
    """
    _GetFWJRTaskName_

    Retrieve the taskName and the fwjr_path to heal corrupted FWJRs
    seen by the JobAccountant. See #5421.
    """
    sql = """
          SELECT wj.fwjr_path, ww.task FROM wmbs_workflow ww
            INNER JOIN wmbs_subscription ws ON ws.workflow = ww.id
            INNER JOIN wmbs_jobgroup wjg ON wjg.subscription = ws.id
            INNER JOIN wmbs_job wj ON wj.jobgroup = wjg.id
            WHERE wj.id = :jobId"""

    def format(self, results):
        """
        _format_

        """
        result = DBFormatter.format(self, results)

        return {"fwjr_path": result[0][0], "taskName": result[0][1]}

    def execute(self, jobId, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"jobId": jobId}, conn = conn,
                                      transaction = transaction)

        return self.format(result)
