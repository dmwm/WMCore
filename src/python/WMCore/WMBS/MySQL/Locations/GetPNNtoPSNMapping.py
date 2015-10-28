#!/usr/bin/env python
"""
_GetPNNtoPSNMapping_

MySQL implementation of Locations.GetPNNtoPSNMapping
"""
from __future__ import print_function
from __future__ import division
from WMCore.Database.DBFormatter import DBFormatter

__all__ = []


class GetPNNtoPSNMapping(DBFormatter):
    """
    Return a python dict mapping PhEDEx node name to list of related
    processing site names.
    """

    sql = """SELECT wls.pnn AS pnn, wl.site_name AS psn FROM wmbs_location_pnns wls
             INNER JOIN wmbs_location wl ON wls.location = wl.id"""


    def execute(self, conn = None, transaction = False):

        results = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        mapping = {}
        for result in self.formatDict(results):
            mapping.setdefault(result['pnn'], []).append(result['psn'])
        return mapping


