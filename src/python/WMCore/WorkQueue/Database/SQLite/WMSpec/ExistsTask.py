"""
_ExistsTask_

Oracle implementation of WMSpec.Exists
"""
__all__ = []
__revision__ = "$Id: ExistsTask.py,v 1.1 2010/08/06 21:05:19 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WMSpec.ExistsTask import ExistsTask \
     as ExistsTaskMySQL
     
class ExistsTask(ExistsTaskMySQL):
    pass
