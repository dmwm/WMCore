#!/usr/bin/env python
"""
_DBSUpload.FindUploadableDatasets_

Find the datasets that have files that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableDatasets.py,v 1.1 2009/07/20 17:51:44 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.FindUploadableDatasets import FindUploadableDatasets as MySQLFindUploadableDatasets

class FindUploadableDatasets(MySQLFindUploadableDatasets):
    
    """
SQLite implementation for finding datasets that have files that need to be uploaded into DBS
    """


