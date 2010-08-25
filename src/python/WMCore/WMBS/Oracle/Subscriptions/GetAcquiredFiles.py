#!/usr/bin/env python
"""
_AcquireFiles_

Oracle implementation of Subscription.GetAcquiredFiles
"""

__all__ = []
__revision__ = "$Id: GetAcquiredFiles.py,v 1.6 2009/09/11 19:07:29 mnorman Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAcquiredFiles import GetAcquiredFiles \
     as GetAcquiredFilesMySQL

class GetAcquiredFiles(GetAcquiredFilesMySQL):
    sql = """SELECT wmsfa.fileid, wl.site_name FROM wmbs_sub_files_acquired wmsfa
             INNER JOIN wmbs_file_location wfl ON wfl.fileid = wmsfa.fileid
             INNER JOIN wmbs_location wl ON wl.id = wfl.location
             WHERE wmsfa.subscription = :subscription
             """
