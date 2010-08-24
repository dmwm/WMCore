#!/usr/bin/env python
"""
_Exists_

SQLite implementation of Files.Exists

"""

__all__ = []



from WMCore.WMBS.MySQL.Files.Exists import Exists as FilesExistsMySQL

class Exists(FilesExistsMySQL):
    sql = FilesExistsMySQL.sql
