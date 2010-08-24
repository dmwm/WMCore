#!/usr/bin/env python
"""
_List_

Oracle implementation of ListLocations

"""
__revision__ = "$Id: List.py,v 1.3 2008/12/05 21:06:24 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Locations.List import List as ListLocationsMySQL

class List(ListLocationsMySQL):
    sql = ListLocationsMySQL.sql
    