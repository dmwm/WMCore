#!/usr/bin/env python
"""
_GetJobs_

MySQL implementation of BossLite.Task.GetJobs
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Job import JobDBFormatter

class GetJobs(DBFormatter):
    """
    BossLite.Task.GetJobs
    """
    
    sql = """SELECT id as id, 
                    job_id as jobId, 
                    task_id as taskId, 
                    wmbs_job_id as wmbsJobId,
                    name as name, 
                    executable as executable, 
                    events as events, 
                    arguments as arguments, 
                    stdin as StandardInput, 
                    stdout as StandardOutput, 
                    stderr as StandardError, 
                    input_files as inputFiles, 
                    output_files as outputFiles, 
                    dls_destination as dlsDestination, 
                    submission_number as submissionNumber, 
                    closed as closed
            FROM bl_job
            WHERE %s """
    
    def execute(self, id, range = None, conn = None, transaction = False):
        """
        Load everything using the database ID
        """
        
        binds = {}
        
        objFormatter = JobDBFormatter()
        
        whereClause = """ task_id = %s """ % (id)
        sqlFilled = self.sql % (whereClause)
        
        if range :
            sqlFilled += """ AND job_id = :jobId """
            binds =[ { 'jobId' : x } for x in range ]
        
        result = self.dbi.processData(sqlFilled, binds, conn = conn,
                                      transaction = transaction)
        
        ppResult = self.formatDict(result)
        return objFormatter.postFormat(ppResult)
