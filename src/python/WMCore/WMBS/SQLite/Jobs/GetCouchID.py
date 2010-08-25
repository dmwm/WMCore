#!/usr/bin/env python
"""
_GetCouchID_

SQLite implementation of Jobs.GetCouchID
"""




from WMCore.WMBS.MySQL.Jobs.GetCouchID import GetCouchID as MySQLGetCouchID

class GetCouchID(MySQLGetCouchID):
    """
    Identical to MySQL version.
    """
    pass
