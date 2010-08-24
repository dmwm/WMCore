#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.AcquireFiles
"""
__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.2 2008/07/21 14:27:05 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL, SQLiteBase):
    sql = AcquireFilesMySQL.sql