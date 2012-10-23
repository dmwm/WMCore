"""
_ListUserContent_

Implementation of ListUserContent for MySQL

"""

from WMCore.Database.DBFormatter import DBFormatter

class ListUserContent(DBFormatter):

    def execute(self, subscription = None, conn = None, transaction = False):

        sql = """SELECT table_name FROM information_schema.tables
                 WHERE table_schema = (SELECT DATABASE())"""

        result = self.dbi.processData(sql, {}, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
