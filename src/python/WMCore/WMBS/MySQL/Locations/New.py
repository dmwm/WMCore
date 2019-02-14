#!/usr/bin/env python
"""
_New_

MySQL implementation of Locations.New
"""

from WMCore.Database.DBFormatter import DBFormatter


class New(DBFormatter):
    sql = """INSERT IGNORE INTO wmbs_location (site_name, ce_name,
                                               pending_slots, running_slots,
                                               plugin, cms_name, state, state_time)
                      SELECT
                      :location AS site_name, :cename AS ce_name,
                      :pending_slots AS pending_slots,
                      :running_slots AS running_slots,
                      :plugin AS plugin,
                      :cmsname AS cms_name,
                      (SELECT id FROM wmbs_location_state WHERE name = 'Normal') AS state,
                      :state_time AS state_time"""

    pnnSQL = "INSERT IGNORE INTO wmbs_pnns (pnn) VALUES (:pnn)"

    mapSQL = """INSERT IGNORE INTO wmbs_location_pnns (location, pnn)
                 SELECT wl.id, wpnn.id FROM wmbs_location wl, wmbs_pnns wpnn
                  WHERE wl.site_name = :location AND wpnn.pnn = :pnn"""

    def execute(self, siteName, cmsName=None, ceName=None, pnn="None",
                runningSlots=0, pendingSlots=0, plugin=None, stateTime=None,
                conn=None, transaction=False):
        """
        _execute_

        Now with 100% more plugin support
        """
        binds = {"location": siteName, "cmsname": cmsName, "cename": ceName,
                 "running_slots": runningSlots, "pending_slots": pendingSlots,
                 "plugin": plugin, "state_time": stateTime}
        self.dbi.processData(self.sql, binds, conn=conn,
                             transaction=transaction)

        binds = {'pnn': pnn}
        self.dbi.processData(self.pnnSQL, binds, conn=conn,
                             transaction=transaction)

        binds = {'location': siteName, 'pnn': pnn}
        self.dbi.processData(self.mapSQL, binds, conn=conn,
                             transaction=transaction)
        return
