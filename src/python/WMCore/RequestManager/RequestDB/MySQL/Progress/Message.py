#!/usr/bin/env python
"""
_Progress.Message_

API for creating a new progress message for a request

"""



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
        self.sql += ":request_id, CURRENT_TIMESTAMP, :message)"
        binds = {"request_id": requestId, "message": message}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return self.format(result)
