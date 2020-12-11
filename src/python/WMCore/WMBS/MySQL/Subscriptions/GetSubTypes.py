#!/usr/bin/env python
"""
_GetSubTypes_

MySQL implementation of Jobs.GetSubTypes
"""

__all__ = []



import logging

from WMCore.Database.DBFormatter import DBFormatter



class GetSubTypes(DBFormatter):
    """
    Get the current sub types available to WMBS

    """

    sql = """SELECT name FROM wmbs_sub_types"""

    def formatThis(self, result):
        """
        It formats stuff

        """

        res = self.format(result)

        final = []

        for item in res:
            if isinstance(item, list):
                final.extend(item)


        return final

    def execute(self, conn = None, transaction = False):
        """
        Find them types

        """

        result = self.dbi.processData(self.sql, binds = {}, conn = conn, transaction = transaction)


        return self.formatThis(result)
