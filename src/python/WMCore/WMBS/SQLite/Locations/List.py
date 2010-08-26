#!/usr/bin/env python
"""
_List_

SQLite implementation of ListLocations
"""

__all__ = []
__revision__ = "$Id: List.py,v 1.4 2009/05/09 12:05:35 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Locations.List import List as ListLocationsMySQL

class List(ListLocationsMySQL):
    sql = ListLocationsMySQL.sql
