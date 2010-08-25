#!/usr/bin/env python
"""
_DBSUpload.FindUploadableDatasets_

Find the datasets that have files that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableDatasets.py,v 1.8 2010/02/24 21:36:59 mnorman Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
from WMCore.Database.DBFormatter import DBFormatter

class FindUploadableDatasets(DBFormatter):
    

    sql = """SELECT das.dataset_id AS ID, ds.Path as Path, das.algo_id as Algo, das.in_dbs as in_dbs,
               da.in_dbs AS algo_in_dbs, das.id AS das_id,
               (SELECT COUNT(*) FROM dbsbuffer_algo_dataset_assoc das2 WHERE das2.dataset_id = das.dataset_id AND das2.in_dbs = 1) AS dataset_in_dbs,
               da.app_name AS ApplicationName, 
               da.app_ver AS ApplicationVersion, 
               da.app_fam AS ApplicationFamily, 
               da.PSet_Hash as PSetHash,
               da.Config_Content as PSetContent
             FROM dbsbuffer_algo_dataset_assoc AS das
             INNER JOIN dbsbuffer_dataset AS ds
              ON das.dataset_id = ds.ID
             INNER JOIN dbsbuffer_algo AS da
              ON das.algo_id = da.ID
             WHERE das.in_dbs = 0"""

    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)

    def makeDS(self, results):
        ret=[]
        for r in results:
            entry={}
            entry['Dataset']=long(r['id'])
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
            if int(r['dataset_in_dbs']) > 0:
                entry['DatasetInDBS'] = True
            else:
                entry['DatasetInDBS'] = False
            path = r['path']
            entry['PrimaryDataset']     = path.split('/')[1]
            entry['ProcessedDataset']   = path.split('/')[2]
            entry['DataTier']           = path.split('/')[3]
            entry['ApplicationName']    = r['applicationname']
            entry['ApplicationVersion'] = r['applicationversion']
            entry['ApplicationFamily']  = r['applicationfamily']
            entry['PSetHash']           = r['psethash']
            entry['PSetContent']        = r['psetcontent']
            ret.append(entry)

        return ret
 
    def execute(self, conn=None, transaction = False):
        binds = self.getBinds()
        result = self.dbi.processData(self.sql, binds, 
                                      conn = conn, transaction = transaction)

        return self.makeDS(self.formatDict(result))

