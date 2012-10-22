"""
_ListUserContent_

Implementation of ListUserContent for Oracle

"""

from WMCore.Database.DBFormatter import DBFormatter

class ListUserContent(DBFormatter):

    def execute(self, subscription = None, conn = None, transaction = False):

        sql = """SELECT object_name FROM user_objects"""

        result = self.dbi.processData(sql, {}, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
