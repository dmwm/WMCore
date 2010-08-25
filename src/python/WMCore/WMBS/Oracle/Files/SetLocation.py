"""
_SetLocation_

Oracle implementation of Files.SetLocation
"""

__revision__ = "$Id: SetLocation.py,v 1.7 2009/10/22 18:38:18 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.WMBS.MySQL.Files.SetLocation import SetLocation as SetLocationMySQL

class SetLocation(SetLocationMySQL):
    sql = """INSERT INTO wmbs_file_location (fileid, location) 
                SELECT :fileid, wmbs_location.id from wmbs_location 
                  WHERE wmbs_location.site_name = :location AND NOT EXISTS
                    (SELECT * FROM wmbs_file_location
                       WHERE fileid = :fileid AND location = wmbs_location.id)"""
