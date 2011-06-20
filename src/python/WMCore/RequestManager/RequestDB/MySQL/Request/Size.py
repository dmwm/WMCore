#!/usr/bin/env python
"""
_Size_

Update request size

"""



from WMCore.Database.DBFormatter import DBFormatter

class Size(DBFormatter):
    """
    _Size_

    Update the event and file size values for a request

    """
    def execute(self, requestId, eventSize, fileSize = None,
                conn = None, trans = False):
        """
        _execute_

        Update the event and file sizes for the request Id provided
        """


        self.sql = "UPDATE reqmgr_request SET request_size_events=:event_size"
        binds = {"event_size": int(eventSize), "request_id": requestId}
        if fileSize != None:
            self.sql += ",request_size_files=:file_size"
            binds["file_size"] = int(fileSize)
        self.sql += " WHERE request_id=:request_id"
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return self.format(result)

