"""
_GetWMSpecInfo_

SQLite implementation of WMSpec.GetWMSpecInfo
"""
__all__ = []
__revision__ = "$Id: GetWMSpecInfo.py,v 1.1 2009/11/20 22:59:59 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WMSpec.GetWMSpecInfo import GetWMSpecInfo \
     as GetWMSpecInfoMySQL

class GetWMSpecInfo(GetWMSpecInfoMySQL):
    sql = GetWMSpecInfoMySQL.sql