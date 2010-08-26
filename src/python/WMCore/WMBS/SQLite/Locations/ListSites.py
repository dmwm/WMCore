#!/usr/bin/env python
"""
_ListSites_

SQLite implementation of Locations.ListSites
"""

__revision__ = "$Id: ListSites.py,v 1.2 2010/04/08 20:09:09 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Locations.ListSites import ListSites as MySQLListSites

class ListSites(MySQLListSites):
    pass
