"""
_Exists_

Oracle implementation of WMSpec.Exists
"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2009/08/18 23:18:13 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.Exists import Exists \
     as ExistsMySQL
     
class Exists(ExistsMySQL):
    sql = ExistsMySQL.sql