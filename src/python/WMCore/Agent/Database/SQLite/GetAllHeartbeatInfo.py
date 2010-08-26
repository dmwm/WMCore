"""
_GetAllHeartbeatInfo_

SQLite implementation of GetAllHeartbeatInfo
"""

__all__ = []
__revision__ = "$Id: GetAllHeartbeatInfo.py,v 1.1 2010/06/21 21:18:48 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.Database.MySQL.GetAllHeartbeatInfo import GetAllHeartbeatInfo \
     as GetAllHeartbeatInfoMySQL

class GetAllHeartbeatInfo(GetAllHeartbeatInfoMySQL):
    pass
