#!/usr/bin/env python
"""
_CompleteFiles_

Oracle implementation of Subscription.CompleteFiles
"""
__all__ = []
__revision__ = "$Id: CompleteFiles.py,v 1.2 2008/11/24 21:51:46 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.CompleteFiles import CompleteFiles as CompleteFilesMySQL

class CompleteFiles(CompleteFilesMySQL):
    sql = """insert into wmbs_sub_files_complete 
                (subscription, fileid) values (:subscription, :fileid)"""