#!/usr/bin/env python
"""
_LoadFromName_

Oracle implementation of Jobs.LoadFromName.
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.LoadFromName import LoadFromName as LoadFromNameJobMySQL

class LoadFromName(LoadFromNameJobMySQL):
    sql = LoadFromNameJobMySQL.sql
