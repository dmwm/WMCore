#!/usr/bin/env python
"""
_GetLocation_

MySQL implementation of Jobs.GetAllJobs
"""

__all__ = []
__revision__ = "$Id: GetAllJobs.py,v 1.2 2009/12/30 14:03:09 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetAllJobs(DBFormatter):
    """
    _GetLocation_

    Retrieve all files that are associated with the given job from the
    database.
    """
    sql_all   = "SELECT id FROM wmbs_job"
    
    sql_state = "SELECT id FROM wmbs_job WHERE state = (SELECT id FROM wmbs_job_state WHERE name = :state)"

    def format(self, results):
        """
        _formatDict_

        Cast the file attribute to an integer, and also handle changing the
        column name in Oracle from FILEID to FILE.
        """

        if len(results) == 0:
            return False
        else:
            tempList = results[0].fetchall()
            final = []
            for i in tempList:
                final.append(i.values()[0])
            return final

        
    def execute(self, state = None, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """
        if state == None or type(state) != str:
            result = self.dbi.processData(self.sql_all, {}, conn = conn,
                                          transaction = transaction)
        else:
            result = self.dbi.processData(self.sql_state, {'state':state.lower()}, conn = conn,
                                          transaction = transaction)
        
        return self.format(result)
