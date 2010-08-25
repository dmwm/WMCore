#!/usr/bin/env python
"""
_Progress.Message_

API for creating a new progress message for a request

"""
__revision__ = "$Id: Message.py,v 1.1 2010/07/01 19:07:54 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Message(DBFormatter):
    """
    _Message_

    Add a status message to a request

    """
    def execute(self, requestId, message, conn = None, trans = False):
        """
        _execute_

        Add a message to the requestId

        """
        self.sql = "INSERT INTO reqmgr_message (request_id, update_time,"
        self.sql += "message) VALUES ("
        self.sql += "%s, CURRENT_TIMESTAMP, \"%s\") " % (requestId, message)

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return self.format(result)

