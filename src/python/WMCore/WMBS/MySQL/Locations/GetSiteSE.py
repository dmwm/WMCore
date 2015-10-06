#!/usr/bin/env python
"""
_GetSiteSE_

MySQL implementation of Locations.GetSiteSE
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

import logging

class GetSiteSE(DBFormatter):
    """
    Grab all the relevant information for a given site.
    Usually useful only in the submitter
    """

    sql = """SELECT wls.se_name AS pnn FROM wmbs_location_senames wls
             INNER JOIN wmbs_location ON wls.location = wmbs_location.id
             WHERE wmbs_location.ce_name = :site"""


    def execute(self, cesite = None, conn = None, transaction = False):

        results = self.dbi.processData(self.sql, {'site': cesite}, conn = conn, transaction = transaction)
        return self.formatDict(results)
