#!/usr/bin/env python
"""
_FailFiles_

SQLite implementation of Subscription.FailFiles
"""
__all__ = []
__revision__ = "$Id: FailFiles.py,v 1.1 2008/10/08 14:30:10 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.FailFiles import FailFiles as FailFilesMySQL

class FailFiles(FailFilesMySQL, SQLiteBase):
    sql = FailFilesMySQL.sql