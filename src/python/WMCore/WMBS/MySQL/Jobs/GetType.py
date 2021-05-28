#!/usr/bin/env python
"""
_GetType_

MySQL implementation of Jobs.GetType
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetType(DBFormatter):
    """
    _GetType_

    Given a job ID, get the type of job from the subscription.
    """
    sql = """SELECT wmbs_job.id AS id, wmbs_sub_types.name AS type
             FROM wmbs_sub_types
               INNER JOIN wmbs_subscription ON
                 wmbs_sub_types.id = wmbs_subscription.subtype
               INNER JOIN wmbs_jobgroup ON
                 wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
             WHERE wmbs_job.id = :jobid"""

    def execute(self, jobID, conn = None, transaction = False):
        isList = isinstance(jobID, list)
        if isList:
            binds = []
            for job in jobID:
                binds.append({"jobid": job})
        else:
            binds = {"jobid": jobID}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        if isList:
            return self.formatDict(result)
        else:
            return self.formatOneDict(result)["type"]
