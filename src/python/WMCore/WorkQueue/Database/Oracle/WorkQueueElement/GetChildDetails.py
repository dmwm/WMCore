"""
_GetChildDetails_

Oracle implementation of WorkQueueElement.GetChildDetails
"""

__all__ = []
__revision__ = "$Id: GetChildDetails.py,v 1.1 2009/11/20 23:00:01 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetChildDetails \
     import GetChildDetails as GetChildDetailsMySQL

class GetChildDetails(GetChildDetailsMySQL):
    """
    The same as MySql implementation
    """
