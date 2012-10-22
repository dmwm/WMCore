#!/usr/bin/env python
"""
_Load_

Oracle implementation of Masks.Load
"""

__all__ = []



from WMCore.WMBS.MySQL.Masks.Load import Load as LoadMasksMySQL

class Load(LoadMasksMySQL):
    sql = LoadMasksMySQL.sql
