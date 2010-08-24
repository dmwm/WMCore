#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.AcquireFiles
"""
__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.4 2008/12/05 21:06:58 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles \
     as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL):
    sql = AcquireFilesMySQL.sql