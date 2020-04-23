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
    sql = """SELECT wl.site_name, wpnn.pnn, wl.ce_name, wl.pending_slots,
                    wl.running_slots, wl.plugin, wl.cms_name, wlst.name AS state
               FROM wmbs_location wl
                 INNER JOIN wmbs_location_pnns wls ON wls.location = wl.id
                 INNER JOIN wmbs_pnns wpnn ON wpnn.id = wls.pnn
                 INNER JOIN wmbs_location_state wlst ON wlst.id = wl.state
          """

    def execute(self, siteName=None, conn=None, transaction=False):
        if not siteName:
            results = self.dbi.processData(self.sql, conn=conn,
                                           transaction=transaction)
        else:
            sql = self.sql + " WHERE wl.site_name = :site"
            results = self.dbi.processData(sql, {'site': siteName},
                                       conn=conn, transaction=transaction)
        return self.formatDict(results)
