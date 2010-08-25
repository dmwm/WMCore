#!/usr/bin/env python
"""
_ExistsByID_

SQLite implementation of Files.Exists
"""

__all__ = []



from WMCore.WMBS.MySQL.Files.ExistsByID import ExistsByID as ExistsByIDJobMySQL

class ExistsByID(ExistsByIDJobMySQL):
    sql = ExistsByIDJobMySQL.sql