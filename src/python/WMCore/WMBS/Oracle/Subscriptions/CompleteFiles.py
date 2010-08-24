#!/usr/bin/env python
"""
_CompleteFiles_

MySQL implementation of Subscription.CompleteFiles
"""
__all__ = []
__revision__ = "$Id: CompleteFiles.py,v 1.1 2008/10/08 14:30:10 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.CompleteFiles import CompleteFiles as CompleteFilesMySQL

class CompleteFiles(CompleteFilesMySQL, SQLiteBase):
    sql = CompleteFilesMySQL.sql