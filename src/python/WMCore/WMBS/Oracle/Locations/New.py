#!/usr/bin/env python
"""
_New_

Oracle implementation of Locations.New
"""

from WMCore.WMBS.MySQL.Locations.New import New as NewLocationMySQL

class New(NewLocationMySQL):
    sql = """INSERT INTO wmbs_location (id, site_name, ce_name, running_slots, pending_slots, plugin, cms_name, state)
               SELECT wmbs_location_SEQ.nextval, :location, :cename, :running_slots AS running_slots,
               :pending_slots AS pending_slots,
               :plugin AS plugin, :cmsname AS cms_name,
               (SELECT id FROM wmbs_location_state where name = 'Normal') AS state
               FROM DUAL
             WHERE NOT EXISTS (SELECT site_name FROM wmbs_location
                               WHERE site_name = :location)"""

    seSQL = """INSERT INTO wmbs_location_pnns (location, se_name)
                 SELECT wl.id, :pnn FROM wmbs_location wl
                 WHERE wl.site_name = :location
                 AND NOT EXISTS (SELECT null FROM wmbs_location_pnns wls2 WHERE
                                  wls2.se_name = :pnn
                                  AND wls2.location = wl.id)
                 """
