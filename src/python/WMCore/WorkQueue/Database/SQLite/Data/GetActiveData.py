"""

SQLite implementation of Block.GetActiveData
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Data.GetActiveData \
    import GetActiveData as GetActiveDataMySQL

class GetActiveData(GetActiveDataMySQL):
    sql = GetActiveDataMySQL.sql
