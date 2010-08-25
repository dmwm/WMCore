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
        UPDATE reqmgr_request SET request_priority = %s
          WHERE request_id = %s
        """ % (priority, requestId)

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return




