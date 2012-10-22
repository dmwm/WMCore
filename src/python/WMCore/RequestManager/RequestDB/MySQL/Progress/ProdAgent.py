#!/usr/bin/env python
"""
_Progress.ProdAgent_

API for logging a Prodagent that is associated with a request

"""



from WMCore.Database.DBFormatter import DBFormatter

class ProdAgent(DBFormatter):
    """
    _ProdAgent_

    """
    def execute(self, requestId, prodagentName, conn = None, trans = False):
        """
        _execute_

        Associate requestId with the prodagent Name provided

        """
        self.sql = "INSERT INTO reqmgr_assigned_prodagent "
        self.sql += "(request_id, prodagent_id) VALUES (:request_id, :prodagent_name)"
        binds = {"request_id": requestId, "prodagent_name": prodagentName}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
