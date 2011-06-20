#!/usr/bin/env python
"""
_Progress.GetProgress_

"""



from WMCore.Database.DBFormatter import DBFormatter

class GetProgress(DBFormatter):
    """
    _GetProgress_

    Get a progress update for a request

    """
    def execute(self, requestId, conn = None, trans = False):

        self.sql = "SELECT * FROM reqmgr_progress_update "
        self.sql += "WHERE request_id=:request_id"
        binds = {"request_id": requestId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return self.formatDict(result)
