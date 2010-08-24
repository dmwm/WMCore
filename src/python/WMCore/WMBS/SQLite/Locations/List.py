#!/usr/bin/env python
"""
_List_

SQLite implementation of ListLocations
"""

__all__ = []



from WMCore.WMBS.MySQL.Locations.List import List as ListLocationsMySQL

class List(ListLocationsMySQL):
    sql = ListLocationsMySQL.sql
