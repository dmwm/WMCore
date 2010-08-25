#!/usr/bin/env python
"""
_SetLocationByLFN_

Oracle implementation of Files.SetLocationByLFN
"""

__revision__ = "$Id: SetLocationByLFN.py,v 1.2 2010/04/08 16:20:09 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Files.SetLocationByLFN import SetLocationByLFN as MySQLSetLocationByLFN

class SetLocationByLFN(MySQLSetLocationByLFN):
    sql = """INSERT INTO wmbs_file_location (fileid, location) 
             SELECT wmbs_file_details.id, wmbs_location.id
               FROM wmbs_location, wmbs_file_details
               WHERE wmbs_location.se_name = :location
               AND wmbs_file_details.lfn = :lfn"""
