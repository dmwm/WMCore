#!/usr/bin/env python
"""
_Progress.Message_
Gets the ProdMgrURL for the event
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetProdMgr(DBFormatter):
    def execute(self, requestId, conn = None, trans = False):
        self.sql = "SELECT prodmgr_id FROM reqmgr_assigned_prodmgr WHERE request_id=:request_id"
        binds = {"request_id": requestId}
        result = self.dbi.processData(self.sql, binds, conn = conn, transaction = trans)
        return self.formatOne(result)
