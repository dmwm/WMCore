#!/usr/bin/env python
"""
_New_

SQLite implementation of Masks.New
"""

__all__ = []



from WMCore.WMBS.MySQL.Masks.New import New as NewMySQL

class New(NewMySQL):
    sql = NewMySQL.sql
