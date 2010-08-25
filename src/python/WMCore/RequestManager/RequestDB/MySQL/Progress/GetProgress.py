#!/usr/bin/env python
"""
_Progress.GetProgress_

"""
__revision__ = "$Id: GetProgress.py,v 1.1 2010/07/01 19:07:54 rpw Exp $"
__version__ = "$Revision: 1.1 $"

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
