#!/usr/bin/env python
"""
_SetLocationByLFN_

Oracle implementation of Files.SetLocationByLFN
"""




from WMCore.WMBS.MySQL.Files.SetLocationByLFN import SetLocationByLFN as MySQLSetLocationByLFN

class SetLocationByLFN(MySQLSetLocationByLFN):
    sql = """INSERT INTO wmbs_file_location (fileid, location) 
             SELECT wmbs_file_details.id, wmbs_location.id
               FROM wmbs_location, wmbs_file_details
               WHERE wmbs_location.se_name = :location
               AND wmbs_file_details.lfn = :lfn"""
