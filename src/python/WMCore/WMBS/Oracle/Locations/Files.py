"""
_Files_

Oracle implementation of Locations.Files

"""

from WMCore.WMBS.MySQL.Locations.Files import Files as FilesLocationsMySQL

class Files(FilesLocationsMySQL):
    sql = FilesLocationsMySQL.sql