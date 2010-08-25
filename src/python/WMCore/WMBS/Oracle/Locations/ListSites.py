#!/usr/bin/env python
"""
_ListSites_

Oracle implementation of Locations.ListSites
"""

__all__ = []
__revision__ = "$Id: ListSites.py,v 1.1 2009/07/01 19:22:14 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Locations.ListSites import ListSites as MySQLListSites

class ListSites(MySQLListSites):
    """
    Right now the same as the MySQL implementation

    """
