"""
_New_

MySQL implementation of Users.New
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT IGNORE INTO wmbs_users (cert_dn, name_hn)
                 VALUES ( :dn, :hn )
          """
    sql_get_id = """SELECT id FROM wmbs_users
                    WHERE cert_dn = :dn
                 """

    def execute(self, dn, hn = None, conn = None, transaction = False):

        binds = {"dn": dn, "hn": hn}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        result = self.dbi.processData( self.sql_get_id, {'dn': dn},
                                       conn = conn, transaction = transaction
                                     )
        id = self.format(result)
        return int(id[0][0])

