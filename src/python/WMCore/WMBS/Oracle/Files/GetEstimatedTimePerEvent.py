#!/usr/bin/env python
"""
_GetEstimatedTimePerEvent_

Oracle implementation of Files.GetEstimatedTimePerEvent
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetEstimatedTimePerEvent(DBFormatter):
    sql = """SELECT time_per_event from wmbs_file_details
             WHERE id = :fileid"""
                                
    def execute(self, binds, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        
        timePerEvent = results[0].fetchall()[0][0]                                
        
        return timePerEvent
