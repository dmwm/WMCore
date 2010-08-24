"""
Oracle implementation of Files.GetBulkLocation
"""




from WMCore.WMBS.MySQL.Files.GetBulkLocation import GetBulkLocation as MySQLGetBulkLocation

class GetBulkLocation(MySQLGetBulkLocation):
    sql = """SELECT wmbs_location.se_name AS site_name, :id AS id
               FROM wmbs_location
               WHERE wmbs_location.id IN (SELECT location FROM wmbs_file_location WHERE fileid = :id)
               """
