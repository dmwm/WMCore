"""
_SetLocation_

Oracle implementation of Files.SetLocation
"""

__revision__ = "$Id: SetLocation.py,v 1.9 2010/07/14 11:41:45 hufnagel Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.WMBS.MySQL.Files.SetLocation import SetLocation as SetLocationMySQL

class SetLocation(SetLocationMySQL):
    sql = """INSERT INTO wmbs_file_location (fileid, location)
             SELECT :fileid, wmbs_location.id FROM wmbs_location 
             WHERE wmbs_location.se_name = :location"""
