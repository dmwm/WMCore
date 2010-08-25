#!/usr/bin/env python
"""
_DBSUpload.FindUploadableFiles_

Find the files in a datasets that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableFiles.py,v 1.3 2009/09/03 12:53:47 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "anzar@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.FindUploadableFiles import FindUploadableFiles as MySQLFindUploadableFiles

class FindUploadableFiles(MySQLFindUploadableFiles):
    
    """
Oracle implementation to find files in datasets that need to be uploaded to DBS
    """
    sql = """SELECT * FROM
               (SELECT dbsbuffer_file.id FROM dbsbuffer_file
                  INNER JOIN dbsbuffer_algo_dataset_assoc ON
                    dbsbuffer_file.dataset_algo = dbsbuffer_algo_dataset_assoc.id
                WHERE status = :status AND
                      dbsbuffer_algo_dataset_assoc.dataset_id = :dataset)
             WHERE rownum <= :maxfiles"""
