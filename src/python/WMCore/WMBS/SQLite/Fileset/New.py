#!/usr/bin/env python
"""
_New_

SQLite implementation of Fileset.New
"""

__all__ = []



from WMCore.WMBS.MySQL.Fileset.New import New as NewFilesetMySQL

class New(NewFilesetMySQL):
    sql = NewFilesetMySQL.sql
