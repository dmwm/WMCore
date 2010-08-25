#!/usr/bin/env python
"""
_ListOpenByName_

SQLite implementation of Fileset.ListOpenByName
"""

__all__ = []



from WMCore.WMBS.MySQL.Fileset.ListOpenByName import ListOpenByName as ListOpenByNameMySQL

class ListOpenByName(ListOpenByNameMySQL):
    sql = ListOpenByNameMySQL.sql
