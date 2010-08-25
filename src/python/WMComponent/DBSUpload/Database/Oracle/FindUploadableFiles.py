#!/usr/bin/env python
"""
_DBSUpload.FindUploadableFiles_

Find the files in a datasets that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableFiles.py,v 1.2 2009/07/20 18:07:26 mnorman Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "anzar@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.FindUploadableFiles import FindUploadableFiles as MySQLFindUploadableFiles

class FindUploadableFiles(MySQLFindUploadableFiles):
    
    """
Oracle implementation to find files in datasets that need to be uploaded to DBS
    """
    

    sql = """SELECT id as ID FROM (SELECT id, status, dataset_algo, ROWNUM rn FROM dbsbuffer_file)
             WHERE dataset_algo = (SELECT ID FROM dbsbuffer_algo_dataset_assoc WHERE dataset_id =:dataset)
             AND status =:status
             AND rn <= :maxfiles""" 
