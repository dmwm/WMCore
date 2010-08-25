"""
Oracle implementation of Files.GetBulkLocation
"""

__revision__ = "$Id: GetBulkLocation.py,v 1.1 2009/09/10 16:10:20 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.GetBulkLocation import GetBulkLocation as MySQLGetBulkLocation

class GetBulkLocation(MySQLGetBulkLocation):

    sql = """SELECT wmbs_location.site_name AS site_name, :id AS id
               FROM wmbs_location
               WHERE wmbs_location.id IN (SELECT location FROM wmbs_file_location WHERE fileid = :id)
               """
