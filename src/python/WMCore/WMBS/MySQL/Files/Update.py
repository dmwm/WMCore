#!/usr/bin/env python
"""
_Update_

MySQL implementation of Files.Update

Created on Feb 25, 2013

@author: dballest
"""

from WMCore.WMBS.MySQL.Files.Add import Add

class Update(Add):
    """
    _Update_

    Update the wmbs_file_details information
    for a file
    """

    sql = """UPDATE wmbs_file_details
                SET filesize = :filesize,
                    events = :events,
                    first_event = :first_event,
                    merged = :merged
                WHERE lfn = :lfn
          """

    def execute(self, files = None, size = 0, events = 0, cksum = 0,
                first_event = 0, merged = False, conn = None,
                transaction = False):
        binds = self.getBinds(files, size, events, cksum, first_event,
                              merged)
        self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return
