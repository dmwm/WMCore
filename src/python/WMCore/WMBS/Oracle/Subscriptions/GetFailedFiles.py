#!/usr/bin/env python
"""
_GetFailedFiles_

Oracle implementation of Subscription.GetFailedFiles
"""

__all__ = []
__revision__ = "$Id: GetFailedFiles.py,v 1.6 2009/09/11 19:07:29 mnorman Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import GetFailedFiles \
     as GetFailedFilesMySQL

class GetFailedFiles(GetFailedFilesMySQL):
    sql = """SELECT wmsff.fileid, wl.site_name FROM wmbs_sub_files_failed wmsff
             INNER JOIN wmbs_file_location wfl ON wfl.fileid = wmsff.fileid
             INNER JOIN wmbs_location wl ON wl.id = wfl.location
             WHERE wmsff.subscription = :subscription
             """
