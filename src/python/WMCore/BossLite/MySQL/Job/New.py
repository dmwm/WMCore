#!/usr/bin/env python
"""
_New_

MySQL implementation of BossLite.Jobs.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.3 2010/05/10 12:57:39 spigafi Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Job import JobDBFormatter
    
class New(DBFormatter):
    """
    BossLite.Jobs.New
    """
    
    sql = """INSERT INTO bl_job (job_id, task_id, name, executable, events,
                arguments, stdin, stdout, stderr, input_files, output_files,
                dls_destination, submission_number, closed)
             VALUES (:jobId, :taskId, :name, :executable, :events, :arguments,
                :standardInput, :standardOutput, :standardError, :inputFiles,
                :outputFiles, :dlsDestination, :submissionNumber, :closed)
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
    
