#!/usr/bin/env python
"""
_LoadFromName_

MySQL implementation of Jobs.LoadFromName.
"""

__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.1 2008/11/20 17:19:00 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromName(DBFormatter):
    """
    _LoadFromName_

    Retrieve meta data for a job given it's name.  This includes the name,
    job group and last update time.  This will also retrieve the job mask
    if one exists.
    """
    sql = "SELECT ID, NAME, JOBGROUP, LAST_UPDATE FROM wmbs_job WHERE NAME = :name"
    
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
               
    def execute(self, name):
        """
        _execute_

        """
        binds = self.getBinds(name = name)
        result = self.dbi.processData(self.sql, binds)
        
        return self.format(result)
