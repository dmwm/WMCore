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
    sql = """SELECT wmbs_location.se_name
             FROM wmbs_file_location
             INNER JOIN wmbs_location ON
               wmbs_location.id = wmbs_file_location.location
             WHERE wmbs_file_location.fileid = (SELECT id FROM wmbs_file_details WHERE lfn = :lfn)"""
