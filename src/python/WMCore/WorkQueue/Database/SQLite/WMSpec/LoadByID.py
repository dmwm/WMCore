"""
_New_

SQLite implementation of Block.LoadByID
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WMSpec.LoadByID import LoadByID \
     as LoadByIDMySQL

class LoadByID(LoadByIDMySQL):
    sql = LoadByIDMySQL.sql