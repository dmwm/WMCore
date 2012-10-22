#!/usr/bin/env python
"""
_ListSubTypes_

Retrieve a list of all subscription types from WMBS.
"""




from WMCore.Database.DBFormatter import DBFormatter

class ListSubTypes(DBFormatter):
    sql = "SELECT name FROM wmbs_sub_types"

    def format(self, result):
        """
        _format_

        Format the query result into a single list of subscription types.
        """
        results = DBFormatter.format(self, result)

        resultList = []
        for result in results:
            resultList.append(result[0])

        return resultList

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.format(result)
