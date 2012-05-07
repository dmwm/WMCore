#!/usr/bin/env python
"""
_New_

Oracle implementation of Locations.New
"""

from WMCore.WMBS.MySQL.Locations.New import New as NewLocationMySQL

class New(NewLocationMySQL):    
    sql = """INSERT INTO wmbs_location (id, site_name, ce_name, job_slots, plugin, cms_name)
               SELECT wmbs_location_SEQ.nextval, :location, :cename, :slots AS job_slots,
               :plugin AS plugin, :cmsname AS cms_name  FROM DUAL
             WHERE NOT EXISTS (SELECT site_name FROM wmbs_location
                               WHERE site_name = :location)"""

    seSQL = """INSERT INTO wmbs_location_senames (location, se_name)
                 SELECT wl.id, :se_name FROM wmbs_location wl
                 WHERE wl.site_name = :location
                 AND NOT EXISTS (SELECT null FROM wmbs_location_senames wls2 WHERE
                                  wls2.se_name = :se_name
                                  AND wls2.location = wl.id)
                 """
