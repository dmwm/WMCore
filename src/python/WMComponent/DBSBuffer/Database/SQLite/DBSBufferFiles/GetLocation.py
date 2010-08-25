#!/usr/bin/env python
"""

SQLite implementation of GetLocation

"""

__revision__ = "$Id: GetLocation.py,v 1.1 2009/05/14 16:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"


from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetLocation import GetLocation as MySQLGetLocation

class GetLocation(MySQLGetLocation):
    """

    SQLite implementation of GetLocation

    """

    def GetUpdateGetLocationDialect(self):

        return 'SQLite'
