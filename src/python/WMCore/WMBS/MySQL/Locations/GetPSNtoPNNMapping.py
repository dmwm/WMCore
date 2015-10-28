#!/usr/bin/env python
"""
_GetPSNtoPNNMapping_

MySQL implementation of Locations.GetPSNtoPNNMapping
"""
from __future__ import print_function
from __future__ import division
from WMCore.Database.DBFormatter import DBFormatter

__all__ = []


class GetPSNtoPNNMapping(DBFormatter):
    """
    Return a python dict mapping processing site name to the list of related
    PhEDEx node names.
    """

    sql = """SELECT wl.site_name AS psn, wls.pnn AS pnn FROM wmbs_location_pnns wls
             INNER JOIN wmbs_location wl ON wls.location = wl.id"""


    def execute(self, conn = None, transaction = False):

        results = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        mapping = {}
        for result in self.formatDict(results):
            mapping.setdefault(result['psn'], []).append(result['pnn'])
        return mapping


