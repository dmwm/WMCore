#!/usr/bin/env python
"""
_List_

Oracle implementation of ListLocations

"""
__all__ = []
__revision__ = "$Id: List.py,v 1.2 2008/11/24 21:51:57 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Locations.List import List as ListLocationsMySQL

class List(ListLocationsMySQL):
    sql = ListLocationsMySQL.sql
    