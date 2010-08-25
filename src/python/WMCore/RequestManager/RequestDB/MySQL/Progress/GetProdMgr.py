#!/usr/bin/env python
"""
_Progress.Message_
Gets the ProdMgrURL for the event
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetProdMgr(DBFormatter):
    def execute(self, requestId, conn = None, trans = False):
        self.sql = "SELECT prodmgr_id FROM reqmgr_assigned_prodmgr WHERE request_id=%s" % requestId
        result = self.dbi.processData(self.sql, conn = conn, transaction = trans)
        return self.formatOne(result)

      
