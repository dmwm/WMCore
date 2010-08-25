"""
_New_

SQLite implementation of Block.GetParentByChildID
"""

__all__ = []
__revision__ = "$Id: GetParentsByChildID.py,v 1.1 2009/09/03 15:44:19 swakef Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.WorkQueue.Database.MySQL.Data.GetParentsByChildID \
    import GetParentsByChildID as GetParentsByChildIDMySQL

class GetParentsByChildID(GetParentsByChildIDMySQL):
    sql = GetParentsByChildIDMySQL.sql
