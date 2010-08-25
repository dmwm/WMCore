#!/usr/bin/env python
"""
_DBSUpload.FindUploadableFiles_

Find the files in a datasets that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableFiles.py,v 1.10 2009/09/25 14:44:42 sfoulkes Exp $"
__version__ = "$Revision: 1.10 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
from WMCore.Database.DBFormatter import DBFormatter

class FindUploadableFiles(DBFormatter):
    sql = """SELECT dbsfile.id as ID FROM dbsbuffer_file dbsfile
             WHERE dbsfile.dataset_algo = (SELECT ID FROM dbsbuffer_algo_dataset_assoc WHERE dataset_id =:dataset)
             AND dbsfile.status =:status
             LIMIT :maxfiles""" 

    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
    
    def getBinds(self, dataset, maxfiles):
        binds =  { 'dataset': dataset['ID'], 'status':'NOTUPLOADED', 'maxfiles': maxfiles }
        return binds

    def makeFile(self, results):
        ret=[]
        for r in results:
                entry={}
                entry['ID']=long(r['id'])
                ret.append(entry)
        return ret

    def execute(self, datasetInfo=None, maxfiles=10, conn=None, transaction = False):

        binds = self.getBinds(datasetInfo, maxfiles)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.makeFile(self.formatDict(result))
    
