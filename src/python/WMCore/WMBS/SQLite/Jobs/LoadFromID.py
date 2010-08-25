#!/usr/bin/env python
"""
_LoadFromID_

SQLite implementation of Jobs.LoadFromID
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.LoadFromID import LoadFromID as LoadFromIDMySQL

class LoadFromID(LoadFromIDMySQL):
    sql = LoadFromIDMySQL.sql
