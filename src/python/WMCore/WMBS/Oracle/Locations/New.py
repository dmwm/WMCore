#!/usr/bin/env python
"""
_New_

Oracle implementation of Locations.New
"""

from WMCore.WMBS.MySQL.Locations.New import New as NewLocationMySQL


class New(NewLocationMySQL):
    sql = """INSERT INTO wmbs_location (id, site_name, ce_name, running_slots, pending_slots,
               plugin, cms_name, state, state_time)
               SELECT wmbs_location_SEQ.nextval, :location, :cename, :running_slots AS running_slots,
               :pending_slots AS pending_slots,
               :plugin AS plugin, :cmsname AS cms_name,
               (SELECT id FROM wmbs_location_state where name = 'Normal') AS state,
               :state_time AS state_time
               FROM DUAL
             WHERE NOT EXISTS (SELECT site_name FROM wmbs_location
                               WHERE site_name = :location)"""

    pnnSQL = """INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX (wmbs_pnns (pnn)) */
                   INTO wmbs_pnns (id, pnn) VALUES (wmbs_pnns_SEQ.nextval, :pnn)"""

    mapSQL = """INSERT INTO wmbs_location_pnns (location, pnn)
                  SELECT (SELECT id from wmbs_location WHERE site_name = :location),
                         (SELECT id from wmbs_pnns WHERE pnn = :pnn) FROM DUAL
                WHERE NOT EXISTS
                    (SELECT * FROM wmbs_location_pnns WHERE
                      location = (SELECT id from wmbs_location WHERE site_name = :location) AND
                      pnn = (SELECT id from wmbs_pnns WHERE pnn = :pnn))"""
