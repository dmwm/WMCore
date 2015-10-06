#!/usr/bin/env python
"""
_GetSiteInfo_

MySQL implementation of Locations.GetSiteInfo
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetSiteInfo(DBFormatter):
    """
    Grab all the relevant information for a given site.
    Usually useful only in the submitter
    """
    sql = """SELECT site_name, wls.se_name as pnn, ce_name, pending_slots, running_slots,
                    plugin, cms_name, wlst.name AS state FROM wmbs_location
               INNER JOIN wmbs_location_senames wls ON wls.location = wmbs_location.id
               INNER JOIN wmbs_location_state wlst ON wlst.id = wmbs_location.state
               WHERE wmbs_location.site_name = :site"""


    def execute(self, siteName = None, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {'site': siteName},
                                       conn = conn, transaction = transaction)
        formatted = self.formatDict(results)
        return formatted
