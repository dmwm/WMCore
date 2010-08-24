#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Jobs.LoadFromID.
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.3 2009/01/13 17:38:27 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    """
    _LoadFromID_

    Retrieve meta data for a job given it's ID.  This includes the name,
    job group and last update time.
    """
    sql = """SELECT ID, NAME, JOBGROUP, LAST_UPDATE FROM wmbs_job
             WHERE ID = :jobid"""
    
    def execute(self, jobID, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """
        result = self.dbi.processData(self.sql, {"jobid": jobID}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)[0]
