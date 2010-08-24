#!/usr/bin/env python
"""
_CompleteFiles_

SQLite implementation of Subscription.CompleteFiles
"""
__all__ = []
__revision__ = "$Id: CompleteFiles.py,v 1.4 2008/12/05 21:06:58 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.CompleteFiles import CompleteFiles \
     as CompleteFilesMySQL

class CompleteFiles(CompleteFilesMySQL):
    sql = CompleteFilesMySQL.sql