#!/usr/bin/env python
"""
_GetLocation_

SQLite implementation of Jobs.Location
"""

__revision__ = "$Id: GetLocation.py,v 1.1 2009/10/02 21:25:23 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.GetLocation import GetLocation as MySQLGetLocation

class GetLocation(MySQLGetLocation):
    """
    _GetLocation_

    Retrieve all files that are associated with the given job from the
    database.
    """
