#!/usr/bin/env python
"""
_Exists_

SQLite implementation of Jobs.Exists
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.Exists import Exists as ExistsMySQL

class Exists(ExistsMySQL):
    sql = ExistsMySQL.sql
