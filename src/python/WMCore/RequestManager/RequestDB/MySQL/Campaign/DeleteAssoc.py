#!/usr/bin/env python
"""
_Assignment.Delete_

Delete the assignment between a campaign and a request

"""





from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """
    _Delete_

    Delete the assignment from a request to a production campaign

    """
    def execute(self, requestId,
                conn = None, trans = False):
        """
        _execute_

        Delete the association between a request and a prod campaign

        """
        self.sql = "DELETE FROM reqmgr_campaign_assoc WHERE request_id = :requestId"
        binds = {"requestId":requestId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return
