"""
Oracle implementation of Files.GetBulkLocation
"""

__revision__ = "$Id: GetBulkLocation.py,v 1.2 2010/04/08 20:09:09 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Files.GetBulkLocation import GetBulkLocation as MySQLGetBulkLocation

class GetBulkLocation(MySQLGetBulkLocation):
    sql = """SELECT wmbs_location.se_name AS site_name, :id AS id
               FROM wmbs_location
               WHERE wmbs_location.id IN (SELECT location FROM wmbs_file_location WHERE fileid = :id)
               """
