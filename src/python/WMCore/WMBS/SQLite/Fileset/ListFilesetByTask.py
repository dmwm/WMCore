#!/usr/bin/env python
"""
_ListFilesetByTask_

SQLite implementation of Fileset.ListFilesetByTask
"""

__all__ = []




from WMCore.WMBS.MySQL.Fileset.ListFilesetByTask import ListFilesetByTask as ListFilesetByTaskMySQL

class ListFilesetByTask(ListFilesetByTaskMySQL):
    sql = ListFilesetByTaskMySQL.sql