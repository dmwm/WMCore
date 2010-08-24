#!/usr/bin/env python
"""
_New_

MySQL implementation of Locations.New
"""

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_location (site_name, se_name, ce_name, job_slots, plugin) 
               SELECT :location AS site_name, :sename AS se_name,
                      :cename AS ce_name, :slots AS job_slots, :plugin as plugin
                      FROM DUAL WHERE NOT EXISTS
                (SELECT site_name FROM wmbs_location WHERE site_name = :location)"""
    
    def execute(self, siteName, jobSlots = 0, seName = None,
                ceName = None, plugin = None, conn = None,
                transaction = False):
        """
        _execute_

        Now with 100% more plugin support
        """
        binds = {"location": siteName, "slots": jobSlots, "sename": seName,
                 "cename": ceName, "plugin": plugin}
        self.dbi.processData(self.sql, binds, conn = conn, 
                             transaction = transaction)
        return
