#!/usr/bin/env python
"""
_ListOpenByName_

Oracle implementation of Fileset.ListOpenByName
"""




from WMCore.WMBS.MySQL.Fileset.ListOpenByName import ListOpenByName as ListOpenFilesetMySQL

class ListOpenByName(ListOpenFilesetMySQL):
    sql = ListOpenFilesetMySQL.sql
