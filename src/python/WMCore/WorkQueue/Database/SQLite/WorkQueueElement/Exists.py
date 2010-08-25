"""
_Exists_

SQLite implementation of WMSpec.Exists
"""
__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.Exists import Exists \
     as ExistsMySQL
     
class Exists(ExistsMySQL):
    sql = ExistsMySQL.sql