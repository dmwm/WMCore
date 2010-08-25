#!/usr/bin/env python
"""
_SetFWJRPath_

MySQL implementation of Jobs.SetFWJRPath
"""

__revision__ = "$Id: SetFWJRPath.py,v 1.1 2009/10/13 20:04:10 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class SetFWJRPath(DBFormatter):
    """
    _SetFWJRPath_

    Update the path to the framework job report for a particular job.
    """
    sql = "UPDATE wmbs_job SET fwjr_path = :fwjrpath WHERE id = :jobid"

    def execute(self, jobID = None, fwjrPath = None, conn = None,
                transaction = False):
        self.dbi.processData(self.sql, {"jobid": jobID, "fwjrpath": fwjrPath},
                             conn = conn, transaction = transaction)
        return
