#!/usr/bin/env python
"""
_Request.Delete_

Delete a request by ID from the database

"""




from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """
    _Delete_

    delete a request by ID

    """
    def execute(self, requestId, conn = None, trans = False):
        """
        _execute_

        delete a request given the request id

        """
        self.sql = "delete from reqmgr_request where request_id = :request_id"
        binds = {"request_id": requestId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return
