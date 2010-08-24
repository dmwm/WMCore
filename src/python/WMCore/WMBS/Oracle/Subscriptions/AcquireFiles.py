#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.AcquireFiles
"""
__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.2 2008/11/24 21:51:46 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL):
    sql = """insert into wmbs_sub_files_acquired 
                (subscription, fileid) values (:subscription, :fileid)"""