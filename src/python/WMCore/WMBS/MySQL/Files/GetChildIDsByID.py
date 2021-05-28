#!/usr/bin/env python
"""
_GetChildIDsByID_

MySQL implementation of File.GetChildIDsByID

Return a list of ids which are children for a file(s) with a given id(s).
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetChildIDsByID(DBFormatter):
    sql = """select distinct child from wmbs_file_parent where parent = :parent"""

    def getBinds(self, ids=None):
        binds = []
        childIDs = self.dbi.makelist(ids)
        for id in childIDs:
            binds.append({'parent': id})
        return binds

    def format(self, result):
        out = set()
        for r in result:
            if isinstance(r, int):
                # deal with crappy mysql implementation
                out.add(int(r))
            else:
                for f in r.fetchall():
                    out.add(int(f[0]))
            r.close()
        return list(out)

    def execute(self, ids=None, conn = None, transaction = False):
        binds = self.getBinds(ids)
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
