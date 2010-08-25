"""
_Exists_

SQLite implementation of WMSpec.Exists
"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2009/08/18 23:18:17 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.WMSpec.Exists import Exists \
     as ExistsMySQL
     
class Exists(ExistsMySQL):
    sql = ExistsMySQL.sql