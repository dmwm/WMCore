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
        self.sql += "WHERE request_id=%s"%requestId
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return self.formatDict(result)
