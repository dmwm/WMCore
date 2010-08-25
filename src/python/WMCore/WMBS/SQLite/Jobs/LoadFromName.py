#!/usr/bin/env python
"""
_LoadFromName_

SQLite implementation of Jobs.LoadFromName
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.LoadFromName import LoadFromName as LoadFromNameMySQL

class LoadFromName(LoadFromNameMySQL):
    sql = LoadFromNameMySQL.sql
