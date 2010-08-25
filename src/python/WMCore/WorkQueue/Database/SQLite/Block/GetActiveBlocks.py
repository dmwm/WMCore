"""

SQLite implementation of Block.GetActiveBlocks
"""

__all__ = []
__revision__ = "$Id: GetActiveBlocks.py,v 1.1 2009/08/18 23:18:13 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Block.GetActiveBlocks \
    import GetActiveBlocks as GetActiveBlocksMySQL

class GetActiveBlocks(GetActiveBlocksMySQL):
    sql = GetActiveBlocksMySQL.sql
