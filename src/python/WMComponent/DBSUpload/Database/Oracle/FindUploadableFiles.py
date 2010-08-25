#!/usr/bin/env python
"""
_DBSUpload.FindUploadableFiles_

Find the files in a datasets that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableFiles.py,v 1.4 2010/02/24 21:38:52 mnorman Exp $"
__version__ = "$Revision: 1.4 $"

from WMComponent.DBSUpload.Database.MySQL.FindUploadableFiles import FindUploadableFiles as MySQLFindUploadableFiles

class FindUploadableFiles(MySQLFindUploadableFiles):
    
    """
Oracle implementation to find files in datasets that need to be uploaded to DBS
    """

