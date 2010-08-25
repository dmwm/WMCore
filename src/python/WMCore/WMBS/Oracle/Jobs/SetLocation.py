#!/usr/bin/env python
"""
_SetLocation_

Oracle implementation of Jobs.SetLocation
"""

__all__ = []
__revision__ = "$Id: SetLocation.py,v 1.1 2010/01/22 17:38:02 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.SetLocation import SetLocation as MySQLSetLocation

class SetLocation(MySQLSetLocation):
    pass
