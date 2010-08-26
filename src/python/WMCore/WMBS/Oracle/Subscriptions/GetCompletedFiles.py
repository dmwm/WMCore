#!/usr/bin/env python
"""
_GetCompletedFiles_

Oracle implementation of Subscription.GetCompletedFiles
"""

__all__ = []
__revision__ = "$Id: GetCompletedFiles.py,v 1.7 2009/09/11 19:07:29 mnorman Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFiles import \
     GetCompletedFiles as GetCompletedFilesMySQL

class GetCompletedFiles(GetCompletedFilesMySQL):
    sql = """SELECT wmsfc.fileid, wl.site_name FROM wmbs_sub_files_complete wmsfc
             INNER JOIN wmbs_file_location wfl ON wfl.fileid = wmsfc.fileid
             INNER JOIN wmbs_location wl ON wl.id = wfl.location
             WHERE wmsfc.subscription = :subscription
             """
