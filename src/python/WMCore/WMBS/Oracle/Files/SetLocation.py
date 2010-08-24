"""
SQLite implementation of SetFileLocation
"""

from WMCore.WMBS.MySQL.Files.SetLocation import SetLocation as SetLocationMySQL
from sets import Set

class SetLocation(SetLocationMySQL):
    
    sql = """insert into wmbs_file_location (fileid, location) 
                values ((select id from wmbs_file_details where lfn = :lfn),
                (select id from wmbs_location where se_name = :location))"""
