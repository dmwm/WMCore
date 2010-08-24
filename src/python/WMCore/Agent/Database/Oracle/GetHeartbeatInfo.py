"""
_GetHeartbeatInfo_

Oracle implementation of GetHeartbeatInfo
"""

__all__ = []



from WMCore.Agent.Database.MySQL.GetHeartbeatInfo import GetHeartbeatInfo \
     as GetHeartbeatInfoMySQL

class GetHeartbeatInfo(GetHeartbeatInfoMySQL):
    pass
