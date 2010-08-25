"""
_GetWMSpecInfo_

Oracle implementation of WMSpec.GetWMSpecInfo
"""
__all__ = []
__revision__ = "$Id: GetWMSpecInfo.py,v 1.1 2009/11/20 23:00:00 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WMSpec.GetWMSpecInfo import GetWMSpecInfo \
     as GetWMSpecInfoMySQL

class GetWMSpecInfo(GetWMSpecInfoMySQL):
    sql = GetWMSpecInfoMySQL.sql