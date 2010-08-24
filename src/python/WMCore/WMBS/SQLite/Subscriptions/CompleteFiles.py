#!/usr/bin/env python
"""
_CompleteFiles_

MySQL implementation of Subscription.CompleteFiles
"""
__all__ = []
__revision__ = "$Id: CompleteFiles.py,v 1.3 2008/11/20 21:54:27 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.CompleteFiles import CompleteFiles as CompleteFilesMySQL

class CompleteFiles(CompleteFilesMySQL):
    sql = CompleteFilesMySQL.sql