#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Jobs.LoadFromID.
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.1 2008/11/20 17:19:00 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    """
    _LoadFromID_

    Retrieve meta data for a job given it's ID.  This includes the name,
    job group and last update time.  This will also retrieve the job mask
    if one exists.
    """
    sql = "SELECT ID, NAME, JOBGROUP, LAST_UPDATE FROM wmbs_job WHERE ID = :jobid"
    
    def format(self, resultProxy):
        """
        _format_

        Format the results of the SQL query into a dictionary with the
        following keys:
          NAME, JOB_GROUP, LAST_UPDATE
        """
        results = resultProxy[0].fetchall()

        out = {}
        out["ID"] = results[0][0]
        out["NAME"] = results[0][1]
        out["JOBGROUP"] = results[0][2]
        out["LAST_UPDATE"] = results[0][3]
        
        return out
               
    def execute(self, jobID):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """
        binds = self.getBinds(jobid = jobID)
        result = self.dbi.processData(self.sql, binds)
        
        return self.format(result)
