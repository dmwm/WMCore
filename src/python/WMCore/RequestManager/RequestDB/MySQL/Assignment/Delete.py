#!/usr/bin/env python
"""
_Assignment.Delete_

Delete the assignment between a team and a request

"""





from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """
    _Delete_

    Delete the assignment from a request to a production team

    """
    def execute(self, requestId,
                conn = None, trans = False):
        """
        _execute_

        Delete the association between a request and a prod team

        """
        binds = {"request_id" : requestId}
        self.sql = "DELETE FROM reqmgr_assignment WHERE request_id = :request_id"
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return
