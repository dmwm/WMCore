#!/usr/bin/env python
"""
_ListCurrentSites_

Query the database to get all the current sites in Resource Control
"""

from WMCore.Database.DBFormatter import DBFormatter

class ListCurrentSites(DBFormatter):
    sql = """SELECT site_name AS site
                    FROM wmbs_location
           """

    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql,
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
