#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Jobs.LoadFromID.
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.4 2009/01/16 22:38:01 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    """
    _LoadFromID_

    Retrieve meta data for a job given it's ID.  This includes the name,
    job group and last update time.
    """
    sql = """SELECT ID, NAME, JOBGROUP, LAST_UPDATE FROM wmbs_job
             WHERE ID = :jobid"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id, jobgroup and last_update columns to integers because
        formatDict() turns everything into strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])
        formattedResult["jobgroup"] = int(formattedResult["jobgroup"])
        formattedResult["last_update"] = int(formattedResult["last_update"])
        return formattedResult
    
    def execute(self, jobID, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """
        result = self.dbi.processData(self.sql, {"jobid": jobID}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
