"""
Oracle implementation of GetLocationFile
"""
from WMCore.WMBS.MySQL.Files.GetLocation import GetLocation \
     as GetLocationFileMySQL

class GetLocation(GetLocationFileMySQL):
    """
    _GetLocation_
    
    Oracle specific: file is reserved word
    """
    
    sql = sql = """select site_name from wmbs_location 
                   where id in (select location from wmbs_file_location 
                   where fileid in (select id from wmbs_file_details where lfn=:lfn))"""
