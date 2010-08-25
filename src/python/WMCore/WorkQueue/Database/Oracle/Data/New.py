"""
_New_

Oracle implementation of Block.New
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Data.New import New \
     as NewMySQL

class New(NewMySQL):
    pass