"""
_SetLocation_

Oracle implementation of Files.SetLocation
"""




from WMCore.WMBS.MySQL.Files.SetLocation import SetLocation as SetLocationMySQL

class SetLocation(SetLocationMySQL):
    sql = """INSERT INTO wmbs_file_location (fileid, location)
             SELECT :fileid, wmbs_location.id FROM wmbs_location 
             WHERE wmbs_location.se_name = :location"""
