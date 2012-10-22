#!/usr/bin/env python
"""
_Request.Priority_

API for adjusting the priority of a request

"""



from WMCore.Database.DBFormatter import DBFormatter

class Priority(DBFormatter):
    """
    _Priority_

    Change the priority of the request provided

    """
    def execute(self, requestId, priority, conn = None, trans = False):
        """
        _execute_

        Add the priorityMod to the requests priority for the request id
        provided.

        """
        self.sql = """
        UPDATE reqmgr_request SET request_priority = :priority
          WHERE request_id = :request_id"""
        binds = {"priority": int(priority), "request_id": requestId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return
