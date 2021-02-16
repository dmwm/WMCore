#!/usr/bin/env python
"""
_ListSites_

MySQL implementation of Locations.ListSites
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

from future.utils import listvalues
import logging

class ListSites(DBFormatter):
    sql = "SELECT site_name FROM wmbs_location"


    def format(self, results):

        if len(results) == 0:
            return False
        else:
            format = []
            for i in results[0].fetchall():
                format.append(listvalues(i)[0])
            return format


    def execute(self, conn = None, transaction = False):

        results = self.dbi.processData(self.sql, {}, conn = conn, transaction = transaction)
        return self.format(results)
