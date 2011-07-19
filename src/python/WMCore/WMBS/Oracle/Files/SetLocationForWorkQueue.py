#!/usr/bin/env python
"""
_SetLocationForWorkQueue_

Oracle implementation of Files.SetLocationForWorkQueue

For WorkQueue only
"""

from WMCore.WMBS.MySQL.Files.SetLocationForWorkQueue import SetLocationForWorkQueue as MySQLSetLocationForWorkQueue

class SetLocationForWorkQueue(MySQLSetLocationForWorkQueue):
    """
    Oracle version


    """
