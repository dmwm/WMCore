#!/usr/bin/env python
"""
_New_

MySQL implementation of Locations.New
"""

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT IGNORE INTO wmbs_location (site_name, ce_name, job_slots, plugin, cms_name) 
               SELECT :location AS site_name, :cename AS ce_name,
                      :slots AS job_slots, :plugin as plugin,
                      :cmsname AS cms_name
                      FROM DUAL"""

    seSQL = """INSERT IGNORE INTO wmbs_location_senames (location, se_name)
                 SELECT id, :se_name FROM wmbs_location WHERE site_name = :location """
    
    def execute(self, siteName, jobSlots = 0, seName = "None",
                ceName = None, plugin = None, cmsName = None,
                conn = None, transaction = False):
        """
        _execute_

        Now with 100% more plugin support
        """
        binds = {"location": siteName, "slots": jobSlots, "cename": ceName,
                 "plugin": plugin, "cmsname": cmsName}
        self.dbi.processData(self.sql, binds, conn = conn, 
                             transaction = transaction)
        binds = {'location': siteName, 'se_name': seName}
        self.dbi.processData(self.seSQL, binds, conn = conn,
                             transaction = transaction)
        return
