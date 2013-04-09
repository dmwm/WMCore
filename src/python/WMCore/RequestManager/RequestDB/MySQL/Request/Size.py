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
    def execute(self, requestId, eventSize, fileSize,
                sizeOfEvent, conn = None, trans = False):
        """
        _execute_

        Update the event and file sizes for the request Id provided
        """

        if eventSize is None:
            eventSize = 0
        if fileSize is None:
            fileSize = 0
        if sizeOfEvent is None:
            sizeOfEvent = 0

        self.sql = """UPDATE reqmgr_request SET request_num_events = :event_size,
                        request_size_files = :file_size,
                        request_event_size = :size_of_event
                        WHERE request_id = :request_id
                    """
        binds = {"event_size": int(eventSize), "request_id": requestId,
                 "file_size" : int(fileSize),
                 "size_of_event" : int(sizeOfEvent)}

        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return self.format(result)
