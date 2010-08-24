#!/usr/bin/env python
"""
_DeleteAcquiredFiles_

SQLite implementation of Subscription.DeleteAcquiredFiles

Remove a (list of) file(s) from the aquired state, either due to a state change 
(e.g file has become completed/failed) or as a clean up/resubmission.
"""

__all__ = []
__revision__ = "$Id: DeleteAcquiredFiles.py,v 1.4 2009/01/12 19:26:06 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.DeleteAcquiredFiles import DeleteAcquiredFiles as DeleteAcquiredFilesMySQL

class DeleteAcquiredFiles(DeleteAcquiredFilesMySQL):
    sql = DeleteAcquiredFilesMySQL.sql
