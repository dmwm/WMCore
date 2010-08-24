"""
_New_

Oracle implementation of WMSpec.New
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WMSpec.New import New \
     as NewMySQL
     
class New(NewMySQL):
    sql = NewMySQL.sql
    