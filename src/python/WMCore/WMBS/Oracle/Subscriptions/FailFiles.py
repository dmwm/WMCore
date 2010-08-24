#!/usr/bin/env python
"""
_FailFiles_

SQLite implementation of Subscription.FailFiles
"""
__all__ = []
__revision__ = "$Id: FailFiles.py,v 1.2 2008/11/24 21:51:46 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.FailFiles import FailFiles as FailFilesMySQL

class FailFiles(FailFilesMySQL):
    sql = """insert into wmbs_sub_files_failed 
                (subscription, fileid) values (:subscription, :fileid)"""