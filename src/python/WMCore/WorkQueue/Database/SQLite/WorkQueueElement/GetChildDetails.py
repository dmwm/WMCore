"""
_GetChildDetails_

SQLite implementation of WorkQueueElement.GetChildDetails
"""

__all__ = []
__revision__ = "$Id: GetChildDetails.py,v 1.1 2009/11/17 16:53:33 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetChildDetails \
     import GetChildDetails as GetChildDetailsMySQL

class GetChildDetails(GetChildDetailsMySQL):
        pass
