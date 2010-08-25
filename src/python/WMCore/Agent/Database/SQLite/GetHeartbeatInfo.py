"""
_GetHeartbeatInfo_

SQLite implementation of GetHeartbeatInfo
"""

__all__ = []
__revision__ = "$Id: GetHeartbeatInfo.py,v 1.1 2010/06/21 21:18:43 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.Database.MySQL.GetHeartbeatInfo import GetHeartbeatInfo \
     as GetHeartbeatInfoMySQL

class GetHeartbeatInfo(GetHeartbeatInfoMySQL):
    pass
