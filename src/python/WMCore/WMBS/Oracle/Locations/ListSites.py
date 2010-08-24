#!/usr/bin/env python
"""
_ListSites_

Oracle implementation of Locations.ListSites
"""

__all__ = []



from WMCore.WMBS.MySQL.Locations.ListSites import ListSites as MySQLListSites

class ListSites(MySQLListSites):
    """
    Right now the same as the MySQL implementation

    """
