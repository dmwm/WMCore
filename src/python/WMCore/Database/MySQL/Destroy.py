"""
_Destroy_

Implementation of Destroy for MySQL

"""

from WMCore.Database.DBFormatter import DBFormatter

class Destroy(DBFormatter):    

    def execute(self, subscription = None, conn = None, transaction = False):

        sql = """SELECT DATABASE() AS dbname"""

        results = self.dbi.processData(sql, {}, conn = conn,
                                       transaction = transaction)

        dbName = self.formatDict(results)[0]['dbname']

        sql = """DROP DATABASE %s""" % dbName

        self.dbi.processData(sql, {}, conn = conn,
                             transaction = transaction)

        sql = """CREATE DATABASE %s""" % dbName

        self.dbi.processData(sql, {}, conn = conn,
                             transaction = transaction)

        sql = """USE %s""" % dbName

        self.dbi.processData(sql, {}, conn = conn,
                             transaction = transaction)

        return
