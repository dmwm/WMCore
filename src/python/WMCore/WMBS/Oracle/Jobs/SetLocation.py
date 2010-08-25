#!/usr/bin/env python
"""
_SetLocation_

Oracle implementation of Jobs.SetLocation
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.SetLocation import SetLocation as MySQLSetLocation

class SetLocation(MySQLSetLocation):
    pass
