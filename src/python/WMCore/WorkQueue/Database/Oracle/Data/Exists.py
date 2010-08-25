"""
_Exists_

Oracle implementation of WMSpec.Exists
"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2010/08/06 21:05:02 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Data.Exists import Exists \
     as ExistsMySQL
     
class Exists(ExistsMySQL):
    pass
