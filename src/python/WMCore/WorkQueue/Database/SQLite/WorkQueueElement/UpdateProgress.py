"""
_UpdateProgress_

SQLite implementation of WorkQueueElement.UpdateProgress
"""

__all__ = []
__revision__ = "$Id: UpdateProgress.py,v 1.1 2010/06/18 15:12:52 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateProgress \
     import UpdateProgress as UpdateProgressMySQL

class UpdateProgress(UpdateProgressMySQL):
    pass
