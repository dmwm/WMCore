#!/usr/bin/env python
"""
_Save_

MySQL implementation of BossLite.Jobs.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.1 2010/03/30 10:13:03 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Save(DBFormatter):
    sql = """UPDATE bl_job SET job_id = :jobId, task_id = :taskId,
                name = :name, executable = :executable, events = :events,
                arguments = :arguments, stdin = :standardInput,
                stdout = :standardOutput, stderr = :standardError,
                input_files = :inputFiles, output_files = :outputFiles,
                dls_destination = :dlsDestination, submission_number = :submissionNumber,
                closed = :closed
                WHERE id = :id
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

