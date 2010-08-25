#!/usr/bin/env python
"""
_ExistsByID_

SQLite implementation of Jobs.Exists
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.ExistsByID import ExistsByID as ExistsByIDJobMySQL

class ExistsByID(ExistsByIDJobMySQL):
    sql = ExistsByIDJobMySQL.sql