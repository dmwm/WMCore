#!/usr/bin/env python
"""
_LoadFiles_

MySQL implementation of Jobs.LoadFiles
"""

__all__ = []
__revision__ = "$Id: LoadFiles.py,v 1.1 2008/11/20 17:19:00 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFiles(DBFormatter):
    """
    _LoadFiles_

    Retrieve all files that are associated with the given job from the
    database.
    """
    sql = "SELECT FILE FROM wmbs_job_assoc WHERE JOB = :jobid"
    
    def format(self, result):
        """
        _format_

        Format the result of the SQL query into a list of file IDs.
        """
        out = []
        for r in result:
            for i in r.fetchall():
                out.append(i[0])
        return out
               
    def execute(self, id):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """        
        binds = self.getBinds(jobid = id)
        result = self.dbi.processData(self.sql, binds)
        
        return self.format(result)
