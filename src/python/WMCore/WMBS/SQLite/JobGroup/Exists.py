#!/usr/bin/env python
"""
_Exists_

SQLite implementation of JobGroup.Exists
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.Exists import Exists as ExistsMySQL

class Exists(ExistsMySQL):
    sql = ExistsMySQL.sql
