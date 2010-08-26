"""
_GetHeartbeatInfo_

Oracle implementation of GetHeartbeatInfo
"""

__all__ = []
__revision__ = "$Id: GetHeartbeatInfo.py,v 1.1 2010/06/21 21:18:16 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.Database.MySQL.GetHeartbeatInfo import GetHeartbeatInfo \
     as GetHeartbeatInfoMySQL

class GetHeartbeatInfo(GetHeartbeatInfoMySQL):
    pass
