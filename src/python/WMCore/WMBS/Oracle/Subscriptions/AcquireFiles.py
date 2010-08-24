#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.AcquireFiles
"""
__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.1 2008/10/08 14:30:10 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL, SQLiteBase):
    sql = AcquireFilesMySQL.sql