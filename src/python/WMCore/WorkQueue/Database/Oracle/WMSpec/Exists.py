"""
_Exists_

Oracle implementation of WMSpec.Exists
"""
__all__ = []



from WMCore.WorkQueue.Database.MySQL.WMSpec.Exists import Exists \
     as ExistsMySQL
     
class Exists(ExistsMySQL):
    sql = ExistsMySQL.sql