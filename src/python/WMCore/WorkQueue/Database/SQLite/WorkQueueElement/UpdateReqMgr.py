"""
_UpdateProgress_

Oracle implementation of WorkQueueElement.UpdateProgress
"""

__all__ = []
__revision__ = "$Id: UpdateReqMgr.py,v 1.1 2010/07/20 13:42:36 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateReqMgr \
     import UpdateReqMgr as UpdateReqMgrMySQL

class UpdateReqMgr(UpdateReqMgrMySQL):
    pass
