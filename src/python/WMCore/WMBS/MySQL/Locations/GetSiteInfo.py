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
        return self.format(results)

    def format(self, result):
        """
        Format the DB results in a plain list of dictionaries, with one
        dictionary for each site name, thus with a list of PNNs.
        :param result: DBResult object
        :return: a list of dictionaries
        """
        # first create a dictionary to make key look-up easier
        resp = {}
        for thisItem in DBFormatter.format(self, result):
            # note that each item has 7 columns returned from the database
            siteName = thisItem[0]
            resp.setdefault(siteName, dict())
            siteInfo = resp[siteName]
            siteInfo['site_name'] = siteName
            siteInfo.setdefault('pnn', [])
            if thisItem[1] not in siteInfo['pnn']:
                siteInfo['pnn'].append(thisItem[1])
            siteInfo['ce_name'] = thisItem[2]
            siteInfo['pending_slots'] = thisItem[3]
            siteInfo['running_slots'] = thisItem[4]
            siteInfo['plugin'] = thisItem[5]
            siteInfo['cms_name'] = thisItem[6]
            siteInfo['state'] = thisItem[7]
        # now return a flat list of dictionaries
        return list(resp.values())
