"""
_New_

MySQL implementation of Users.New
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT IGNORE INTO wmbs_users (cert_dn, name_hn, owner, grp, group_name, role_name)
                 VALUES (:dn, :hn, :owner, :grp, :gr, :role)
          """
    sql_get_id = """SELECT id FROM wmbs_users
                    WHERE cert_dn = :dn
                    AND group_name = :gr
                    AND role_name = :role
                 """

    def execute(self, dn, hn = None, owner = None, group = None,
                group_name = '', role_name = '',
                conn = None, transaction = False):

        binds = {"dn": dn, "hn": hn, "owner": owner, "grp": group,
                 "gr": group_name, "role": role_name}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        result = self.dbi.processData( self.sql_get_id, {'dn': dn,
                                                         "gr": group_name,
                                                         "role": role_name},
                                       conn = conn, transaction = transaction )
        id = self.format(result)
        return int(id[0][0])
