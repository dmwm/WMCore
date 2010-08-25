#!/usr/bin/env python
"""
_ListOpen_

Oracle implementation of Fileset.ListOpen
"""

__all__ = []



from WMCore.WMBS.MySQL.Fileset.ListOpen import ListOpen as ListOpenFilesetMySQL

class ListOpen(ListOpenFilesetMySQL):
    sql = ListOpenFilesetMySQL.sql
