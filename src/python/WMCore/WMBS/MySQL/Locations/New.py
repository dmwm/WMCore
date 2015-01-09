#!/usr/bin/env python
"""
_New_

MySQL implementation of Locations.New
"""

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT IGNORE INTO wmbs_location (site_name, ce_name,
                                               pending_slots, running_slots,
                                               plugin, cms_name, state)
                      SELECT
                      :location AS site_name, :cename AS ce_name,
                      :pending_slots AS pending_slots,
                      :running_slots AS running_slots,
                      :plugin as plugin,
                      :cmsname AS cms_name,
                      (SELECT id FROM wmbs_location_state WHERE name = 'Normal') AS state"""

    seSQL = """INSERT IGNORE INTO wmbs_location_senames (location, se_name)
                 SELECT id, :pnn FROM wmbs_location WHERE site_name = :location """

    def execute(self, siteName, runningSlots = 0, pendingSlots = 0,
                pnn = "None",
                ceName = None, plugin = None, cmsName = None,
                conn = None, transaction = False):
        """
        _execute_

        Now with 100% more plugin support
        """
        binds = {"location": siteName, "pending_slots": pendingSlots,
                 "running_slots": runningSlots, "cename": ceName,
                 "plugin": plugin, "cmsname": cmsName}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        binds = {'location': siteName, 'pnn': pnn}
        self.dbi.processData(self.seSQL, binds, conn = conn,
                             transaction = transaction)
        return
