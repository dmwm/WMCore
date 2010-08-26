#!/usr/bin/env python
"""
_Progress.ProdAgent_

API for logging a Prodagent that is associated with a request

"""
__revision__ = "$Id: ProdAgent.py,v 1.1 2010/07/01 19:07:54 rpw Exp $"
__version__ = "$Revision: 1.1 $"

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
        self.sql += "(request_id, prodagent_id) VALUES (%s, \'%s\')" % (
            requestId, prodagentName)

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)


