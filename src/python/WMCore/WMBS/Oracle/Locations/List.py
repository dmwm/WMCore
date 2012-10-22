#!/usr/bin/env python
"""
_List_

Oracle implementation of ListLocations
"""




from WMCore.WMBS.MySQL.Locations.List import List as ListLocationsMySQL

class List(ListLocationsMySQL):
    sql = ListLocationsMySQL.sql
