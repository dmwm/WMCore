#!/usr/bin/env python
"""
_LoadFromID_

SQLite implementation of JobGroup.LoadFromID
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.LoadFromID import LoadFromID as LoadFromIDMySQL

class LoadFromID(LoadFromIDMySQL):
    sql = LoadFromIDMySQL.sql
