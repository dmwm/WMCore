#!/usr/bin/env python
"""
MySQL implementation of File.SetParentage

Make the parentage link between two file lfns in bulk
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetParentage(DBFormatter):
    sql = """INSERT IGNORE INTO wmbs_file_parent (child, parent)
             SELECT DISTINCT wfd1.id, wfd2.id
             FROM wmbs_file_details wfd1 INNER JOIN wmbs_file_details wfd2
             WHERE wfd1.lfn = :child
             AND wfd2.lfn = :parent
    """

    def execute(self, binds, conn = None, transaction = False):
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
