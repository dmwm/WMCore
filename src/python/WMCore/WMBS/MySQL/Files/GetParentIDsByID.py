#!/usr/bin/env python
"""
_GetParentIDsByID_

MySQL implementation of File.GetParentIDsByID

Return a list of ids which are parents for a file with a given id(s).
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetParentIDsByID(DBFormatter):
    sql = """select distinct parent from wmbs_file_parent where child = :child"""

    def getBinds(self, ids=None):
        binds = []
        childIDs = self.dbi.makelist(ids)
        for id in childIDs:
            binds.append({'child': id})
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
