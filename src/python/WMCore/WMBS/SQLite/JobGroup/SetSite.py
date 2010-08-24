#!/usr/bin/env python
"""
_GetSite_

SQLite implementation of JobGroup.GetSite
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.SetSite import SetSite as MySQLSetSite

class SetSite(MySQLSetSite):
    """
    SQLite implementation

    """

    sql = """UPDATE wmbs_jobgroup
              SET location = (SELECT ID FROM wmbs_location WHERE site_name = :site_name)
              WHERE ID = :jobGroupID"""
