#!/usr/bin/env python
"""
_SetCouchID_

SQLite implementation of Jobs.SetCouchID
"""




from WMCore.WMBS.MySQL.Jobs.SetCouchID import SetCouchID as MySQLSetCouchID

class SetCouchID(MySQLSetCouchID):
    """
    Identical to MySQL version.
    """
    pass
