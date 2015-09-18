"""
_Create_

Implementation of Create for MySQL

"""

from WMCore.Database.DBFormatter import DBFormatter

class Create(DBFormatter):

    def execute(self, dbName, conn = None):
        """Execute create statement"""
        # check among list of database if dbName is present
        sql = """SHOW DATABASES"""
        results = self.dbi.processData(sql, {}, conn = conn)
        found = False
        for row in self.formatDict(results):
            if row['database'] == dbName:
                found = True
                break
        if  not found:
            try:
                sql = """CREATE DATABASE %s""" % dbName
                self.dbi.processData(sql, {}, conn = conn)
            except Exception as exp:
                print("Create database: %s" % str(exp))
                raise exp
        sql = """USE %s""" % dbName
        self.dbi.processData(sql, {}, conn = conn)
