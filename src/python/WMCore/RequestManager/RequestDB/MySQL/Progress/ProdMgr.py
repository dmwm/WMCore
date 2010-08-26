#!/usr/bin/env python
"""
_Progress.ProdMgr_

API for logging a ProdMgr that is associated with a request

"""
__revision__ = "$Id: ProdMgr.py,v 1.1 2010/07/01 19:07:54 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class ProdMgr(DBFormatter):
    """
    _ProdMgr_

    """
    def execute(self, requestId, prodMgrName, conn = None, trans = False):
        """
        _execute_

        Associate requestId with the prodMgr Name provided

        """
        self.sql = "INSERT INTO reqmgr_assigned_prodmgr "
        self.sql += "(request_id, prodmgr_id) VALUES (%s, \'%s\')" % (
            requestId, prodMgrName)

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)


