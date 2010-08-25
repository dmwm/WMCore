"""

Orcale implementation of Block.GetActiveData
"""

__all__ = []
__revision__ = "$Id: GetActiveData.py,v 1.1 2009/09/03 15:44:17 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Data.GetActiveData \
    import GetActiveData as GetActiveDataMySQL

class GetActiveData(GetActiveDataMySQL):
    sql = GetActiveDataMySQL.sql
