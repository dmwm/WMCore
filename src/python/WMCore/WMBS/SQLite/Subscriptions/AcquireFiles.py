#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.AcquireFiles
"""
__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.3 2008/11/20 21:54:27 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL):
    sql = AcquireFilesMySQL.sql