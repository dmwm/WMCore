#/usr/bin/env python
"""
_Destroy_

SQLite implementation of ResourceControl.Destroy.
"""




from WMCore.ResourceControl.MySQL.Destroy import Destroy as MySQLDestroy

class Destroy(MySQLDestroy):
    pass
