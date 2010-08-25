"""
Oracle implementation of SetFileLocation
"""

from WMCore.WMBS.MySQL.Files.SetLocation import SetLocation as SetLocationMySQL
from sets import Set

class SetLocation(SetLocationMySQL):
    
    sql = """insert into wmbs_file_location (fileid, location) 
                SELECT wmbs_file_details.id, wmbs_location.id from wmbs_file_details, wmbs_location 
                 WHERE wmbs_file_details.lfn = :lfn
                 AND wmbs_location.se_name = :location 
                 AND NOT EXISTS (SELECT * FROM wmbs_file_location WHERE fileid = wmbs_file_details.id
                 and location = wmbs_location.id)"""
