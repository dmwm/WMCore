#!/usr/bin/env python
"""
_AcquireFiles_

Oracle implementation of Subscription.AcquireFiles
"""
__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.3 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles \
     as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL):
    sql = """insert into wmbs_sub_files_acquired 
                (subscription, fileid) values (:subscription, :fileid)"""