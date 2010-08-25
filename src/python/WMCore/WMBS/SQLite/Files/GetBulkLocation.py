#!/usr/bin/env python

"""
SQLite implementation of Files.GetBulkLocation
"""

__revision__ = "$Id: GetBulkLocation.py,v 1.1 2009/09/10 16:10:20 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.GetBulkLocation import GetBulkLocation as MySQLGetBulkLocation

class GetBulkLocation(MySQLGetBulkLocation):
    pass

