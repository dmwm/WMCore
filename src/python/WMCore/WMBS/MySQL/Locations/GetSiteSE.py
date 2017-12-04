#!/usr/bin/env python
"""
_GetSiteSE_

MySQL implementation of Locations.GetSiteSE
"""

__all__ = []

from WMCore.Database.DBFormatter import DBFormatter


class GetSiteSE(DBFormatter):
    """
    Grab all the relevant information for a given site.
    Usually useful only in the submitter
    """

    sql = """SELECT wpnn.pnn FROM wmbs_location_pnns wlpnn
               INNER JOIN wmbs_location wl ON wlpnn.location = wl.id
               INNER JOIN wmbs_pnns wpnn ON wlpnn.pnn = wpnn.id
             WHERE wl.ce_name = :site"""

    def execute(self, cesite=None, conn=None, transaction=False):
        results = self.dbi.processData(self.sql, {'site': cesite}, conn=conn, transaction=transaction)
        return self.formatDict(results)
