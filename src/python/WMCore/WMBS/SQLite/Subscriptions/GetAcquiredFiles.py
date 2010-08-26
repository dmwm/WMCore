#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAcquiredFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetAcquiredFiles.py,v 1.3 2008/12/05 21:06:58 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAcquiredFiles import GetAcquiredFiles \
     as GetAcquiredFilesMySQL

class GetAcquiredFiles(GetAcquiredFilesMySQL):
    sql = GetAcquiredFilesMySQL.sql

