#!/usr/bin/env python
"""
_GetCompletedFiles_

Oracle implementation of Subscription.GetCompletedFiles
"""

__all__ = []
__revision__ = "$Id: GetCompletedFiles.py,v 1.5 2009/03/16 16:58:38 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFiles import \
     GetCompletedFiles as GetCompletedFilesMySQL

class GetCompletedFiles(GetCompletedFilesMySQL):
    sql = """SELECT fileid FROM wmbs_sub_files_complete 
             WHERE subscription=:subscription AND rownum <= :maxfiles"""
