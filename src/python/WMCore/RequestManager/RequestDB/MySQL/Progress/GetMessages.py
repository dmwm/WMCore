#!/usr/bin/env python
"""
_Progress.Message_
Gets progress messages for a request

"""
__revision__ = "$Id: GetMessages.py,v 1.1 2010/07/01 19:07:53 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetMessages(DBFormatter):
    def execute(self, requestId, conn = None, trans = False):
        self.sql = "SELECT message FROM reqmgr_message WHERE request_id=%s" % requestId
        result = self.dbi.processData(self.sql, conn = conn, transaction = trans)
        return self.format(result)

      
