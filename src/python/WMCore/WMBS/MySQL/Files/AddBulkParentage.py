#!/usr/bin/env python
"""
_AddBulkParentage_

MySQL implementation of Files.AddBulkParentage
"""

__revision__ = "$Id: AddBulkParentage.py,v 1.1 2009/12/17 22:34:14 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class AddBulkParentage(DBFormatter):
    sql = "INSERT INTO wmbs_file_parent (child, parent) VALUES (:child, :parent)"
    
    def execute(self, fileParentage, conn = None, transaction = False):
        self.dbi.processData(self.sql, fileParentage, conn = conn,
                             transaction = transaction)
        return
