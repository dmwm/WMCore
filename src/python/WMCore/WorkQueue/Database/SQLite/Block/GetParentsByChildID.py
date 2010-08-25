"""
_New_

SQLite implementation of Block.GetParentByChildID
"""

__all__ = []
__revision__ = "$Id: GetParentsByChildID.py,v 1.1 2009/07/17 14:25:29 swakef Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.WorkQueue.Database.MySQL.Block.GetParentsByChildID \
    import GetParentsByChildID as GetParentsByChildIDMySQL

class GetParentsByChildID(GetParentsByChildIDMySQL):
    sql = GetParentsByChildIDMySQL.sql