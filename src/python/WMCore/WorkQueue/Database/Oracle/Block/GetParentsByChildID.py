"""
_New_

Oracle implementation of Block.GetParentByChildID
"""

__all__ = []
__revision__ = "$Id: GetParentsByChildID.py,v 1.2 2009/08/18 23:18:16 swakef Exp $"
__version__ = "$Revision: 1.2 $"

import time
from WMCore.WorkQueue.Database.MySQL.Block.GetParentsByChildID \
    import GetParentsByChildID as GetParentsByChildIDMySQL

class GetParentsByChildID(GetParentsByChildIDMySQL):
    sql = GetParentsByChildIDMySQL.sql