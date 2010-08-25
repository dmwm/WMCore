#!/usr/bin/env python
"""
_DBSUpload.FindUploadableDatasets_

Find the datasets that have files that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableDatasets.py,v 1.2 2009/07/20 18:07:26 mnorman Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "anzar@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.FindUploadableDatasets import FindUploadableDatasets as MySQLFindUploadableDatasets

class FindUploadableDatasets(MySQLFindUploadableDatasets):
    
    """
Oracle implementation for finding datasets that have files that need to be uploaded into DBS
    """


    sql = """SELECT das.dataset_id as ID, ds.Path as Path, das.algo_id as Algo, das.in_dbs as in_dbs 
             FROM dbsbuffer_algo_dataset_assoc das
             INNER JOIN dbsbuffer_dataset ds
               ON das.dataset_id = ds.ID
             WHERE das.ID IN (SELECT df.dataset_algo FROM dbsbuffer_file df WHERE df.status = 'NOTUPLOADED')
             """
