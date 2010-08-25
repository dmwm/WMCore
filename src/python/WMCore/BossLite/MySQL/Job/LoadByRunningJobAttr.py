#!/usr/bin/env python
"""
_LoadByRunningJobAttr_

MySQL implementation of BossLite.Jobs.LoadByRunningJobAttr
"""

__all__ = []
__revision__ = "$Id: LoadByRunningJobAttr.py,v 1.2 2010/05/17 19:08:56 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Job import JobDBFormatter

class LoadByRunningJobAttr(DBFormatter):
    """
    BossLite.Jobs.LoadByRunningJobAttr
    """
    
    sql = """SELECT bl_job.id as id,
                bl_job.job_id as jobId, 
                bl_job.task_id as taskId,
                bl_job.name as name, 
                bl_job.executable as executable, 
                bl_job.events as events,
                bl_job.arguments as arguments, 
                bl_job.stdin as StandardInput,
                bl_job.stdout as StandardOutput, 
                bl_job.stderr as StandardError,
                bl_job.input_files as inputFiles, 
                bl_job.output_files as outputFiles,
                bl_job.dls_destination as dlsDestination,
                bl_job.submission_number as submissionNumber,
                bl_job.closed as closed
                FROM bl_job
                INNER JOIN bl_runningjob ON 
                        bl_runningjob.job_id = bl_job.job_id
                WHERE bl_runningjob.submission = 
                        ( SELECT MAX(submission) FROM bl_runningjob 
                            WHERE bl_runningjob.job_id = bl_job.job_id )
                        AND bl_runningjob.%s = :value """

    def execute(self, column, value, conn = None, transaction = False):
        """
        Load a job based on the attributes of the most recent runningJob
        """
        
        objFormatter = JobDBFormatter()
        
        if type(value) == list:
            binds = value
        else:
            binds = {'value': value}

        sql = self.sql % (column)
        
        result = self.dbi.processData(sql, binds, conn = conn,
                                      transaction = transaction)
        
        ppResult = self.formatDict(result)
        return objFormatter.postFormat(ppResult)
