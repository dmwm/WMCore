#!/usr/bin/env python
"""
_InsertType_

MySQL implementation of Subscriptions.InsertType
"""




from WMCore.Database.DBFormatter import DBFormatter

class InsertType(DBFormatter):
    sql = """INSERT INTO wmbs_sub_types (name)
               SELECT :name AS name FROM DUAL WHERE NOT EXISTS
                (SELECT name FROM wmbs_sub_types WHERE name = :name)"""
    
    def execute(self, subType, conn = None, transaction = False):
        self.dbi.processData(self.sql, {"name": subType}, conn = conn, 
                             transaction = transaction)
        return
