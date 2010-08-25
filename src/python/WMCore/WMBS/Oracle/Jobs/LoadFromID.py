#!/usr/bin/env python
"""
_LoadFromID_

Oracle implementation of Jobs.LoadFromID.
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.LoadFromID import LoadFromID as LoadFromIDJobMySQL

class LoadFromID(LoadFromIDJobMySQL):
    sql = LoadFromIDJobMySQL.sql
