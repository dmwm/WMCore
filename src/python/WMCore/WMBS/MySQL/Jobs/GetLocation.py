#!/usr/bin/env python
"""
_GetLocation_

MySQL implementation of Jobs.Location
"""

__all__ = []
__revision__ = "$Id: GetLocation.py,v 1.1 2009/10/02 21:25:22 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetLocation(DBFormatter):
    """
    _GetLocation_

    Retrieve all files that are associated with the given job from the
    database.
    """
    sql = "SELECT site_name FROM wmbs_location WHERE id = (SELECT location FROM wmbs_job WHERE JOB = :jobid)"
    bulkSQL = """SELECT wmbs_location.site_name as site_name, wmbs_job.name as name
                 FROM wmbs_location
                 INNER JOIN wmbs_job
                 ON wmbs_location.id = wmbs_job.location
                 WHERE wmbs_job.id = :jobid"""

    def format(self, results):
        """
        _formatDict_

        """

        if len(results) == 0:
            return False
        else:
            return results[0][0]

        
    def execute(self, id, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """
        if type(id) == list:
            result = self.dbi.processData(self.bulkSQL, id, conn = conn, transaction = transaction)
            return self.formatDict(result)
        result = self.dbi.processData(self.sql, {"jobid": id}, conn = conn,
                                      transaction = transaction)
        
        return self.format(result)
