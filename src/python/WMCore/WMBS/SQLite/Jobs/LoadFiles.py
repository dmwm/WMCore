#!/usr/bin/env python
"""
_LoadFiles_

SQLite implementation of Jobs.LoadFiles
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.LoadFiles import LoadFiles as LoadFilesMySQL

class LoadFiles(LoadFilesMySQL):
    sql = LoadFilesMySQL.sql
