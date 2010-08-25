#!/usr/bin/env python
"""
_Destroy_

Install the TestDB schema for SQLite.
"""


from WMQuality.TestDB.MySQL.Destroy import Destroy as MySQLDestroy

class Destroy(MySQLDestroy):
    pass
