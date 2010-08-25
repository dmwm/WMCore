#!/usr/bin/env python
"""
_DBSUpload.FindUploadableFiles_

Find the files in a datasets that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableFiles.py,v 1.1 2009/07/20 17:51:44 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.FindUploadableFiles import FindUploadableFiles as MySQLFindUploadableFiles

class FindUploadableFiles(MySQLFindUploadableFiles):
    
    """
SQLite implementation to find files in datasets that need to be uploaded to DBS
    """
    

