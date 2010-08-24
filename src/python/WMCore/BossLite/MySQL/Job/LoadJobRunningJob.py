#!/usr/bin/env python
"""
_Load_

MySQL implementation of BossLite.Job.LoadJobRunningJob
This is being used by JobTracker algorithm
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.DbObject import DbObjectDBFormatter

class LoadJobRunningJob(DBFormatter):
    """
    BossLite.Job.LoadJobRunningJob
    """
   
    sql = """SELECT bl_job.id as id, 
                    bl_job.job_id as jobId, 
                    bl_job.task_id as taskId,
                    bl_job.wmbs_job_id as wmbsJobId,
                    bl_job.name as name, 
                    bl_runningjob.submission AS submission, 
                    bl_runningjob.status AS status, 
                    bl_runningjob.lb_timestamp AS lbTimestamp
                FROM bl_job
                INNER JOIN bl_runningjob ON 
                        (bl_runningjob.job_id = bl_job.job_id AND 
                         bl_runningjob.task_id = bl_job.task_id AND
                         bl_runningjob.submission = bl_job.submission_number)
                WHERE bl_runningjob.closed = 'N' and bl_job.closed = 'N'"""


    def execute(self, conn = None, transaction = False):
        """
        Load everything using the database ID
        """
        objFormatter = LocalFormatter()

        result = self.dbi.processData(self.sql, {}, conn = conn,
                                      transaction = transaction)
        part = self.formatDict(result)
        return objFormatter.postFormat(part)


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
            result['id']               = entry['id']
            result['jobId']            = entry['jobid']
            result['taskId']           = entry['taskid']
            result['wmbsJobId']        = entry['wmbsjobid']
            result['name']             = entry['name']
            result['status']           = entry['status']
            result['lbTimestamp']      = entry['lbtimestamp']
            result['submission']       = entry['submission']

            final.append(result)

        return final

