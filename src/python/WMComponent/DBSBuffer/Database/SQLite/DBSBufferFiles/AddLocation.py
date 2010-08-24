#!/usr/bin/env python
"""
_AddLocation_

SQLite implementation of DBSBufferFiles.AddLocation
"""

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.Transaction import Transaction

class AddLocation(DBFormatter):
    sql = """INSERT INTO dbsbuffer_location (se_name) 
               SELECT :location AS se_name WHERE NOT EXISTS
                (SELECT se_name FROM dbsbuffer_location WHERE se_name = :location)"""

    existsSQL = """SELECT se_name, id FROM dbsbuffer_location
                   WHERE se_name = :location"""

    def execute(self, siteName, conn = None, transaction = False):

        if type(siteName) == str:
            binds = {"location": siteName}
        else:
            binds = []
            for aLocation in siteName:
                binds.append({"location": aLocation})

        myTransaction = Transaction(self.dbi)
        myTransaction.begin()

        nameMap = {}
        self.dbi.processData(self.sql, binds, conn = conn, 
                             transaction = transaction)
        results = self.dbi.processData(self.existsSQL, binds,
                                           conn = myTransaction.conn,
                                           transaction = True)
        results = self.format(results)
        for result in results:
            nameMap[result[0]] = int(result[1])

        myTransaction.commit()
        return nameMap
