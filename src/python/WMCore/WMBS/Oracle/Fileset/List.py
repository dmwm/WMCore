#!/usr/bin/env python
"""
_List_

Oracle implementation of ListFileset

"""
__all__ = []



from WMCore.WMBS.MySQL.Fileset.List import List as ListFilesetMySQL

class List(ListFilesetMySQL):
    sql = ListFilesetMySQL.sql

    def format(self, result):
        """
        Some standardised formatting
        """
        out = []
        for r in result:
            for i in r.fetchall():
                res = i
                j = i[0], str(i[1]), self.truefalse(i[2]), i[3]
                out.append(j)
        return out
