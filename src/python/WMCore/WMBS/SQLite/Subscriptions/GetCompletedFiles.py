#!/usr/bin/env python
"""
_GetCompletedFiles_

SQLite implementation of Subscription.GetCompletedFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetCompletedFiles.py,v 1.4 2008/12/05 21:06:58 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFiles import \
     GetCompletedFiles as GetCompletedFilesMySQL

class GetCompletedFiles(GetCompletedFilesMySQL):
    sql = GetCompletedFilesMySQL.sql

