"""
_GetWMSpecInfo_

Oracle implementation of WMSpec.GetWMSpecInfo
"""
__all__ = []



from WMCore.WorkQueue.Database.MySQL.WMSpec.GetWMSpecInfo import GetWMSpecInfo \
     as GetWMSpecInfoMySQL

class GetWMSpecInfo(GetWMSpecInfoMySQL):
    sql = GetWMSpecInfoMySQL.sql