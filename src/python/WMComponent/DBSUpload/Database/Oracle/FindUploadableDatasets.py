#!/usr/bin/env python
"""
_DBSUpload.FindUploadableDatasets_

Find the datasets that have files that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableDatasets.py,v 1.3 2010/02/24 21:36:59 mnorman Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "anzar@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.FindUploadableDatasets import FindUploadableDatasets as MySQLFindUploadableDatasets

class FindUploadableDatasets(MySQLFindUploadableDatasets):
    
    """
Oracle implementation for finding datasets that have files that need to be uploaded into DBS
    """

