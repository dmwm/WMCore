#!/usr/bin/env python
"""
_Destroy_

Install the TestDB schema for SQLite.
"""

__revsion__ = "$Id: Destroy.py,v 1.2 2010/08/18 15:38:54 meloam Exp $"


from WMQuality.TestDB.MySQL.Destroy import Destroy as MySQLDestroy

class Destroy(MySQLDestroy):
    pass
