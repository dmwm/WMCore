#!/usr/bin/env python
"""
_LoadFromName_

Oracle implementation of LoadFileset
"""

__all__ = []



from WMCore.WMBS.MySQL.Fileset.LoadFromName import LoadFromName as LoadFilesetMySQL

class LoadFromName(LoadFilesetMySQL):
    sql = LoadFilesetMySQL.sql
