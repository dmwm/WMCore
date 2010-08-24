#!/usr/bin/env python
"""
_SetFWJRPath_

MySQL implementation of Jobs.SetFWJRPath
"""




from WMCore.Database.DBFormatter import DBFormatter

class SetFWJRPath(DBFormatter):
    """
    _SetFWJRPath_

    Update the path to the framework job report for a particular job.
    """
    sql = "UPDATE wmbs_job SET fwjr_path = :fwjrpath WHERE id = :jobid"

    def execute(self, jobID = None, fwjrPath = None, conn = None,
                transaction = False, binds = None):
        """
        Send either a jobID and a path, or a list of dictionaries
        with jobid and fwjrpath values.

        """
        if not binds:
            binds = {"jobid": jobID, "fwjrpath": fwjrPath}
        self.dbi.processData(self.sql, binds,
                             conn = conn, transaction = transaction)
        return
