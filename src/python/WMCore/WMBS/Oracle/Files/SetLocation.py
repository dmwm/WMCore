"""
_SetLocation_

Oracle implementation of Files.SetLocation
"""

__revision__ = "$Id: SetLocation.py,v 1.8 2010/04/08 20:09:09 sfoulkes Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.WMBS.MySQL.Files.SetLocation import SetLocation as SetLocationMySQL

class SetLocation(SetLocationMySQL):
    sql = """INSERT INTO wmbs_file_location (fileid, location) 
                SELECT :fileid, wmbs_location.id from wmbs_location 
                  WHERE wmbs_location.se_name = :location AND NOT EXISTS
                    (SELECT * FROM wmbs_file_location
                       WHERE fileid = :fileid AND location = wmbs_location.id)"""
