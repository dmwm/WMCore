#!/usr/bin/env python
"""
_GetCache_

MySQL implementation of Jobs.GetState
"""

__all__ = []
__revision__ = "$Id: GetCache.py,v 1.1 2009/09/09 19:13:53 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetCache(DBFormatter):
    """
    _GetState_

    Given a job ID, get the state of a current job.
    """
    sql = "SELECT cache_dir FROM wmbs_job WHERE id = :jobid"

    def format(self, results):
        """
        _formatDict_

        Return a name from MySQL
        """

        if len(results) == 0:
            return False
        else:
            return results[0].fetchall()[0].values()[0]

        
    def execute(self, ID, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """        
        result = self.dbi.processData(self.sql, {"jobid": ID}, conn = conn,
                                      transaction = transaction)
        
        return self.format(result)
