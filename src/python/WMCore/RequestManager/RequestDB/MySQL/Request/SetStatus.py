#!/usr/bin/env python
"""
_Request.SetStatus_

Change a requests status

"""



from WMCore.Database.DBFormatter import DBFormatter

class SetStatus(DBFormatter):
    """
    _SetStatus_

    Update a requests status value to a new value


    """
    def execute(self, requestId, statusId, conn = None, trans = False):
        """
        _execute_

        Update reqmgr_request status field to the new status value

        """
        self.sql = """
        UPDATE reqmgr_request SET request_status=:status_id
          WHERE request_id = :request_id"""
        binds = {"status_id": statusId, "request_id": requestId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return
