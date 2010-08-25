"""
_LoadByBlockID_

Oracle implementation of Site.LoadByBlockID
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.LoadByDataID import LoadByDataID \
     as LoadByDataIDMySQL

class LoadByDataID(LoadByDataIDMySQL):
    sql = LoadByDataIDMySQL.sql
