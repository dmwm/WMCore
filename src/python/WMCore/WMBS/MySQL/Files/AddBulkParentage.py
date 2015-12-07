#!/usr/bin/env python
"""
_AddBulkParentage_

MySQL implementation of Files.AddBulkParentage
"""




from WMCore.Database.DBFormatter import DBFormatter

class AddBulkParentage(DBFormatter):
    sql = "INSERT INTO wmbs_file_parent (child, parent) VALUES (:child, :parent)"

    def execute(self, fileParentage, conn = None, transaction = False):
        self.dbi.processData(self.sql, fileParentage, conn = conn,
                             transaction = transaction)
        return
