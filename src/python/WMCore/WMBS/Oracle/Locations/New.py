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

    pnnSQL = """INSERT INTO wmbs_pnns (id, pnn)
                  SELECT wmbs_pnns_SEQ.nextval, :pnn
                  FROM DUAL WHERE NOT EXISTS
                (SELECT pnn FROM wmbs_pnns where pnn = :pnn)"""

    mapSQL = """INSERT INTO wmbs_location_pnns (location, pnn)
                  SELECT wl.id, wpnn.id FROM wmbs_location wl, wmbs_pnns wpnn
                  WHERE wl.site_name = :location AND wpnn.pnn = :pnn
                AND NOT EXISTS
                  (SELECT null FROM wmbs_location wl2, wmbs_pnns wpnn2
                  WHERE wl2.site_name = :location AND wpnn2.pnn = :pnn)"""