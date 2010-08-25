#!/usr/bin/env python
"""
_MarkLocation_

SQLite implementation of Subscription.MarkLocation
"""

__revision__ = "$Id: MarkLocation.py,v 1.5 2010/02/09 17:44:04 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Subscriptions.MarkLocation import MarkLocation as MySQLMarkLocation

class MarkLocation(MySQLMarkLocation):
    pass
