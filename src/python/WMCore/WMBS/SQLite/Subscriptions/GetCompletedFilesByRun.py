#!/usr/bin/env python
"""
_GetCompletedFilesByRun_

SQLite implementation of Subscription.GetCompletedFilesByRun
"""

__all__ = []
__revision__ = "$Id: GetCompletedFilesByRun.py,v 1.1 2009/05/01 19:42:49 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFilesByRun import \
     GetCompletedFilesByRun as GetCompletedFilesByRunMySQL

class GetCompletedFilesByRun(GetCompletedFilesByRunMySQL):
    sql = GetCompletedFilesByRunMySQL.sql
