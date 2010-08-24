#!/usr/bin/env python
"""
_LoadFromUID_

SQLite implementation of JobGroup.LoadFromUID
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.LoadFromUID import LoadFromUID as LoadFromUIDMySQL

class LoadFromUID(LoadFromUIDMySQL):
    sql = LoadFromUIDMySQL.sql
