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

        if dbName == None or dbName == 'None':
            # Then we have no database.
            # This presents us with a problem.  We've been asked to clear a
            # non-existant DB.
            # Obviously we can't drop it, so what we have to do is try
            # to pull the URL from the threaded dbi and use that
            # to create the proper db
            dbName = self.dbi.engine.url.database
        else:
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
