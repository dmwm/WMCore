#!/usr/bin/env python
"""
_ExistsByID_

Oracle implementation of Files.Exists
"""

__all__ = []



from WMCore.WMBS.MySQL.Files.ExistsByID import ExistsByID as ExistsByIDMySQL

class ExistsByID(ExistsByIDMySQL):
    sql = ExistsByIDMySQL.sql
