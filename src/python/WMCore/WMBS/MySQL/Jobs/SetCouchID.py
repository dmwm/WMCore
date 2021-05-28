#!/usr/bin/env python
"""
_SetCouchID_

MySQL implementation of Jobs.SetCouchID
"""




from WMCore.Database.DBFormatter import DBFormatter

class SetCouchID(DBFormatter):
    """
    _SetCouchID_

    Update the id of the couch document for the given job.
    """
    sql = "UPDATE wmbs_job SET couch_record = :couchid WHERE id = :jobid"

    def execute(self, jobID = None, couchID = None, bulkList = None, conn = None,
                transaction = False):
        """
        _execute_

        Update the location of the couch record for the job.
        """

        if isinstance(bulkList, list):
            binds = bulkList
        else:
            binds = {"jobid": jobID, "couchid": couchID}

        self.dbi.processData(self.sql, binds,
                             conn = conn, transaction = transaction)
        return
