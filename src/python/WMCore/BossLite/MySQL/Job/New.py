#!/usr/bin/env python
"""
_New_

MySQL implementation of BossLite.Jobs.New
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Job import JobDBFormatter
    
class New(DBFormatter):
    """
    BossLite.Jobs.New
    """
    
    sql = """INSERT INTO bl_job (job_id, task_id, wmbs_job_id, name, executable,
                events, arguments, stdin, stdout, stderr, input_files,
                output_files, dls_destination, submission_number, closed)
             VALUES (:jobId, :taskId, :wmbsJobId, :name, :executable, :events,
                :arguments, :standardInput, :standardOutput, :standardError,
                :inputFiles, :outputFiles, :dlsDestination, :submissionNumber,
                :closed)
                """

    def execute(self, binds, conn = None, transaction = False):
        """
        execute DAO
        """
        
        objFormatter = JobDBFormatter()
        
        ppBinds = objFormatter.preFormat(binds)
        
        self.dbi.processData(self.sql, ppBinds, conn = conn,
                             transaction = transaction)
        
        # try to catch error code?
        return
    
