#!/usr/bin/env python
"""
_LoadForOutput_

MySQL implementation of BossLite.Jobs.LoadForOutput
"""


from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.DbObject import DbObjectDBFormatter

from WMCore.BossLite.DbObjects.Job import JobDBFormatter

from WMCore.BossLite.DbObjects.RunningJob import RunningJob

class LoadForOutput(DBFormatter):
    """
    BossLite.Jobs.LoadForOutput
    """

    sql = """SELECT rj.task_id as taskId,
                    rj.job_id as jobId
             FROM bl_runningjob rj
                INNER JOIN bl_job j ON
                      (rj.submission = j.submission_number AND
                       rj.job_id = j.job_id AND 
                       rj.task_id = j.task_id)
             WHERE rj.status = :status AND
                   rj.process_status <> 'output_retrieved'
             GROUP BY rj.task_id, rj.job_id
             ORDER BY rj.task_id, rj.job_id
             LIMIT :limit"""

    def execute(self, status, limit = 1, conn = None, transaction = False):
        """
        Load a job based on the attributes of the most recent runningJob
        """

        objFormatter = LocalFormatter()

        binds = {'status': status, 'limit': limit }

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        ppResult = self.formatDict(result)
        return objFormatter.postFormat(ppResult)

class LocalFormatter(DbObjectDBFormatter):
    """
    LocalFormatter class used for the specific jobtracker query
    """

    def postFormat(self, res):
        """
        Format the results into the right output. This is useful for any 
        kind of database engine!
        """

        final = []
        for entry in res:
            result = {}
            result['jobId']            = entry['jobid']
            result['taskId']           = entry['taskid']

            final.append(result)

        return final

