"""
_New_

Oracle implementation of Block.LoadByID
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Data.LoadByID import LoadByID \
     as LoadByIDMySQL

class LoadByID(LoadByIDMySQL):
    sql = LoadByIDMySQL.sql
