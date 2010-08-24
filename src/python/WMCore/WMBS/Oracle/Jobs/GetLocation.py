#!/usr/bin/env python
"""
_GetLocation_

Oracle implementation of Jobs.Location
"""




from WMCore.WMBS.MySQL.Jobs.GetLocation import GetLocation as MySQLGetLocation

class GetLocation(MySQLGetLocation):
    """
    _GetLocation_

    Retrieve all files that are associated with the given job from the
    database.
    """
