#!/usr/bin/env python
"""
_DBSUpload.FindUploadableFiles_

Find the files in a datasets that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableFiles.py,v 1.12 2010/05/26 19:21:46 mnorman Exp $"
__version__ = "$Revision: 1.12 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
from WMCore.Database.DBFormatter import DBFormatter

class FindUploadableFiles(DBFormatter):
    sql = """SELECT dbsfile.id as ID, das.dataset_id AS dataset, ds.Path as Path, das.algo_id as Algo, das.in_dbs as in_dbs,
               das.id AS das_id,
               da.app_name AS ApplicationName, 
               da.app_ver AS ApplicationVersion, 
               da.app_fam AS ApplicationFamily, 
               da.PSet_Hash as PSetHash,
               da.Config_Content as PSetContent,
               da.in_dbs AS algo_in_dbs
             FROM dbsbuffer_file dbsfile
             INNER JOIN dbsbuffer_algo_dataset_assoc das ON dbsfile.dataset_algo = das.id
             INNER JOIN dbsbuffer_dataset ds ON ds.id = das.dataset_id
             INNER JOIN dbsbuffer_algo da ON da.id = das.algo_id
             AND dbsfile.status =:status
             """ 

    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
    
    def getBinds(self):
        binds =  {'status':'NOTUPLOADED'}
        return binds

    def makeFile(self, results):
        ret=[]
        for r in results:
            entry={}
            entry['ID']=long(r['id'])
            entry['Path']=r['path']
            entry['DAS_ID'] = long(r['das_id'])
            if not r['algo'] == None:
                entry['Algo'] = int(r['algo'])
            else:
                entry['Algo'] = None
            if not r['algo_in_dbs'] == None:
                entry['AlgoInDBS'] = int(r['algo_in_dbs'])
            else:
                entry['AlgoInDBS'] = None
            if not r['in_dbs'] == None:
                entry['DASInDBS'] = int(r['in_dbs'])
            else:
                entry['DASInDBS'] = None
            path = r['path']
            entry['PrimaryDataset']     = path.split('/')[1]
            entry['ProcessedDataset']   = path.split('/')[2]
            entry['DataTier']           = path.split('/')[3]
            entry['ApplicationName']    = r['applicationname']
            entry['ApplicationVersion'] = r['applicationversion']
            entry['ApplicationFamily']  = r['applicationfamily']
            entry['PSetHash']           = r['psethash']
            entry['PSetContent']        = r['psetcontent']
            entry['Dataset']            = r['dataset']
            ret.append(entry)

        return ret


    def execute(self, conn=None, transaction = False):

        binds = self.getBinds()
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        
        return self.makeFile(self.formatDict(result))

    
