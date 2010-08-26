#!/usr/bin/env python
"""
_GetFailedFiles_

SQLite implementation of Subscription.GetFailedFiles
"""

__all__ = []
__revision__ = "$Id: GetFailedFilesByRun.py,v 1.1 2009/05/01 19:42:49 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFailedFilesByRun import \
     GetFailedFilesByRun as GetFailedFilesByRunMySQL

class GetFailedFilesByRun(GetFailedFilesByRunMySQL):
    sql = GetFailedFilesByRunMySQL.sql 