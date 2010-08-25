#!/usr/bin/env python
"""
_LoadFromID_

SQLite implementation of LoadFileset
"""

__all__ = []



from WMCore.WMBS.MySQL.Fileset.LoadFromID import LoadFromID as LoadFilesetMySQL

class LoadFromID(LoadFilesetMySQL):
    sql = LoadFilesetMySQL.sql
