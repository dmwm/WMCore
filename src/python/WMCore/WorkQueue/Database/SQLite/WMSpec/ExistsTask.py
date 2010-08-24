"""
_ExistsTask_

Oracle implementation of WMSpec.Exists
"""
__all__ = []



from WMCore.WorkQueue.Database.MySQL.WMSpec.ExistsTask import ExistsTask \
     as ExistsTaskMySQL
     
class ExistsTask(ExistsTaskMySQL):
    pass
