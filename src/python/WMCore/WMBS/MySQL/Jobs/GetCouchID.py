#!/usr/bin/env python
"""
_GetCouchID_

MySQL implementation of Jobs.GetCouchID
"""

__revision__ = "$Id: GetCouchID.py,v 1.1 2009/09/16 20:17:05 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetCouchID(DBFormatter):
    """
    _GetCouchID_

    Given a job ID retrieve the couch document ID.
    """
    sql = "SELECT couch_record FROM wmbs_job WHERE id = :jobid"

    def format(self, results):
        """
        _format_

        Return the couch document ID or None if one has not been set.
        """
        result = DBFormatter.format(self, results)

        if len(result) == 0:
            return None

        return result[0][0]
        
    def execute(self, jobID, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """        
        result = self.dbi.processData(self.sql, {"jobid": jobID}, conn = conn,
                                      transaction = transaction)
        
        return self.format(result)
