"""
_New_

Oracle implementation of Block.GetParentByChildID
"""

__all__ = []
__revision__ = "$Id: GetParentsByChildID.py,v 1.1 2009/06/25 18:55:51 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.WorkQueue.Database.MySQL.Block.GetParentsByChildID \
    import GetParentsByChildID as GetParentsByChildIDMySQL

class GetParentsByChildID(GetParentsByChildIDMySQL):
    sql = GetParentsByChildIDMySQL.sql