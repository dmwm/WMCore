#!/usr/bin/env python
"""
_Destroy_

Install the TestDB schema for SQLite.
"""

__revsion__ = "$Id: Destroy.py,v 1.1 2010/03/01 16:49:03 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMQuality.TestDB.MySQL.Destroy import Destroy as MySQLDestroy

class Destroy(MySQLDestroy):
    pass
