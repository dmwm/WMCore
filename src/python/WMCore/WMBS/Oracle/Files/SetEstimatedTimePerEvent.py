#!/usr/bin/env python
"""
_SetEstimatedTimePerEvent_

Oracle implementation of Files.SetEstimatedTimePerEvent
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetEstimatedTimePerEvent(DBFormatter):
    sql = """UPDATE wmbs_file_details
             SET time_per_event = :time_per_event
             WHERE id = :fileid"""
                                
    def execute(self, binds, conn = None, transaction = False):
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)                                

        return
