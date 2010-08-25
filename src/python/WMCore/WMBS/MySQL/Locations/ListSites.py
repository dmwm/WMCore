#!/usr/bin/env python
"""
_ListSites_

MySQL implementation of Locations.ListSites
"""

__all__ = []
__revision__ = "$Id: ListSites.py,v 1.1 2009/07/01 19:22:13 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

import logging

class ListSites(DBFormatter):
    sql = "SELECT site_name FROM wmbs_location"


    def format(self, results):

        if len(results) == 0:
            return False
        else:
            format = []
            for i in results[0].fetchall():
                format.append(i.values()[0])
            return format


    def execute(self, conn = None, transaction = False):

        results = self.dbi.processData(self.sql, {}, conn = conn, transaction = transaction)
        return self.format(results)
