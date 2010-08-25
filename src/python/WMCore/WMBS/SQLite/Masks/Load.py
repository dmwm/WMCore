#!/usr/bin/env python
"""
_Load_

SQLite implementation of Masks.Load
"""

__all__ = []



from WMCore.WMBS.MySQL.Masks.Load import Load as LoadMySQL

class Load(LoadMySQL):
    sql = LoadMySQL.sql
