#!/usr/bin/env python
"""
_ExistsByID_

SQLite implementation of JobGroup.ExistsByID
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.ExistsByID import ExistsByID as ExistsByIDJobGroupMySQL

class ExistsByID(ExistsByIDJobGroupMySQL):
    sql = ExistsByIDJobGroupMySQL.sql