#!/usr/bin/env python
"""
_GetFailedFiles_

Oracle implementation of Subscription.GetFailedFiles
"""

__all__ = []
__revision__ = "$Id: GetFailedFiles.py,v 1.4 2009/03/16 16:58:38 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import GetFailedFiles \
     as GetFailedFilesMySQL

class GetFailedFiles(GetFailedFilesMySQL):
    sql = """SELECT fileid FROM wmbs_sub_files_failed 
             WHERE subscription=:subscription AND rownum <= :maxfiles"""
