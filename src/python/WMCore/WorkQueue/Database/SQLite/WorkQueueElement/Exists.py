"""
_Exists_

SQLite implementation of WMSpec.Exists
"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2009/07/17 14:25:28 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.Exists import Exists \
     as ExistsMySQL
     
class Exists(ExistsMySQL):
    sql = ExistsMySQL.sql