#!/usr/bin/env python
"""
_FailFiles_

Oracle implementation of Subscription.FailFiles
"""
__all__ = []
__revision__ = "$Id: FailFiles.py,v 1.3 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.FailFiles import FailFiles \
     as FailFilesMySQL

class FailFiles(FailFilesMySQL):
    sql = """insert into wmbs_sub_files_failed 
                (subscription, fileid) values (:subscription, :fileid)"""