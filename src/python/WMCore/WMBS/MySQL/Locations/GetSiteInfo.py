#!/usr/bin/env python
"""
_GetSiteInfo_

MySQL implementation of Locations.GetSiteInfo
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

import logging

class GetSiteInfo(DBFormatter):
    """
    Grab all the relevant information for a given site.
    Usually useful only in the submitter
    """
    
    sql = """SELECT site_name, wls.se_name, ce_name, job_slots, plugin, cms_name, drain FROM wmbs_location
               INNER JOIN wmbs_location_senames wls ON wls.location = wmbs_location.id
               WHERE wmbs_location.site_name = :site"""


    def execute(self, siteName = None, conn = None, transaction = False):

        results   = self.dbi.processData(self.sql, {'site': siteName}, conn = conn, transaction = transaction)
        formatted = self.formatDict(results)
        for entry in formatted:
            if entry['drain'] == 'T':
                entry['drain'] = True
            else:
                entry['drain'] = False
        return formatted
