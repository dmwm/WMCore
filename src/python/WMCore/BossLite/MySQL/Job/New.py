#!/usr/bin/env python
"""
_Create_

MySQL implementation of BossLite.Jobs.Create
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2010/03/30 10:13:38 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO bl_job (job_id, task_id, name, executable, events,
                arguments, stdin, stdout, stderr, input_files, output_files,
                dls_destination, submission_number, closed)
             VALUES (:jobId, :taskId, :name, :executable, :events, :arguments,
                :standardInput, :standardOutput, :standardError, :inputFiles,
                :outputFiles, :dlsDestination, :submissionNumber, :closed)
                """


    def execute(self, binds, conn = None, transaction = False):
        """
        This assumes that you are passing in binds in the same format
        as BossLite.DbObjects.Job, and that you already have an id.  It was
        too long a function for me to want to write in Perugia while
        parsing the binds
        """
        
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
    
