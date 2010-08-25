#!/usr/bin/env python
"""
_SetLocationByLFN_

Oracle implementation of Files.SetLocationByLFN
"""

__revision__ = "$Id: SetLocationByLFN.py,v 1.1 2010/03/09 19:59:26 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.SetLocationByLFN import SetLocationByLFN as MySQLSetLocationByLFN

class SetLocationByLFN(MySQLSetLocationByLFN):
    sql = """INSERT INTO wmbs_file_location (fileid, location) 
             SELECT wmbs_file_details.id, wmbs_location.id
               FROM wmbs_location, wmbs_file_details
               WHERE wmbs_location.site_name = :location
               AND wmbs_file_details.lfn = :lfn"""
