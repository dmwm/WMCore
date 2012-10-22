#!/usr/bin/env python

"""
MySQL implementation of AddCKType
"""





from WMCore.Database.DBFormatter import DBFormatter

class AddCKType(DBFormatter):
    sql = """INSERT INTO wmbs_checksum_type (type)
               VALUES (:cktype)"""

    def execute(self, cktype = None, conn = None, transaction = False):

        binds = {'cktype': cktype}

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return
