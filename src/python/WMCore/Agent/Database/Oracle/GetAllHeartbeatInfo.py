"""
_GetAllHeartbeatInfo_

Oracle implementation of GetAllHeartbeatInfo
"""

__all__ = []



from WMCore.Agent.Database.MySQL.GetAllHeartbeatInfo import GetAllHeartbeatInfo \
     as GetAllHeartbeatInfoMySQL

class GetAllHeartbeatInfo(GetAllHeartbeatInfoMySQL):
    pass
