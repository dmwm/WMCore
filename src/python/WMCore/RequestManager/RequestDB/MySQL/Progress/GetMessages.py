#!/usr/bin/env python
"""
_Progress.Message_
Gets progress messages for a request

"""



from WMCore.Database.DBFormatter import DBFormatter

class GetMessages(DBFormatter):
    def execute(self, requestId, conn = None, trans = False):
        self.sql = "SELECT message FROM reqmgr_message WHERE request_id=:request_id"
        binds = {"request_id": requestId}
        result = self.dbi.processData(self.sql, binds, conn = conn, transaction = trans)
        return self.format(result)
