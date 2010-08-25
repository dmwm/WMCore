#!/usr/bin/env python
"""
_GetJobs_

MySQL implementation of BossLite.Task.GetJobs
"""

__all__ = []
__revision__ = "$Id: GetJobs.py,v 1.4 2010/05/10 12:54:43 spigafi Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Job import JobDBFormatter

class GetJobs(DBFormatter):
    """
    BossLite.Task.GetJobs
    """
    
    sql = """SELECT id as id, 
                    job_id as jobId, 
                    task_id as taskId, 
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
            WHERE task_id = :taskId
            """
    
    def execute(self, binds, conn = None, transaction = False):
        """
        Load everything using the database ID
        """
        
        objFormatter = JobDBFormatter()
        
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        
        ppResult = self.formatDict(result)
        return objFormatter.postFormat(ppResult)
