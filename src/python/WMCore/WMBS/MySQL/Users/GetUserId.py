"""
_GetUserId_

MySQL implementation of Users.GetUserId
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter

class GetUserId(DBFormatter):
    sql = """SELECT id FROM wmbs_users
             WHERE cert_dn = :dn
          """

    def execute(self, dn = None, conn = None, transaction = False):
        binds = {"dn": dn}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        id = self.format(result)
        if len(id) > 0:
            return int(id[0][0])

        return None


