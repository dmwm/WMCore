#!/usr/bin/env python
"""
_LoadFiles_

MySQL implementation of Jobs.LoadFiles
"""

__all__ = []
__revision__ = "$Id: LoadFiles.py,v 1.3 2009/01/13 17:38:27 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFiles(DBFormatter):
    """
    _LoadFiles_

    Retrieve all files that are associated with the given job from the
    database.
    """
    sql = "SELECT FILE FROM wmbs_job_assoc WHERE JOB = :jobid"

    def execute(self, id, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """        
        result = self.dbi.processData(self.sql, {"jobid": id}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
