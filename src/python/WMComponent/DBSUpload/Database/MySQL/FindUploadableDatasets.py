#!/usr/bin/env python
"""
_DBSUpload.FindUploadableDatasets_

Find the datasets that have files that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableDatasets.py,v 1.7 2009/07/20 18:02:53 mnorman Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
from WMCore.Database.DBFormatter import DBFormatter

class FindUploadableDatasets(DBFormatter):
    
    sql = """SELECT ds.id as ID, ds.Path as Path, ds.Algo as Algo, ds.AlgoInDBS as AlgoInDBS 
    						FROM dbsbuffer_dataset ds WHERE UnMigratedFiles > 0"""
    sql = """SELECT das.dataset_id as ID, ds.Path as Path, das.algo_id as Algo, das.in_dbs as in_dbs 
             FROM dbsbuffer_algo_dataset_assoc AS das
             INNER JOIN dbsbuffer_dataset AS ds
               ON das.dataset_id = ds.ID
             WHERE das.ID IN (SELECT df.dataset_algo FROM dbsbuffer_file AS df WHERE df.status = 'NOTUPLOADED')
    """
    #sql = """SELECT ds.id as ID, ds.Path as Path, ds.Algo as Algo, ds.AlgoInDBS as AlgoInDBS FROM dbsbuffer_dataset ds """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)

    def makeDS(self, results):
        ret=[]
        for r in results:
            entry={}
            entry['ID']=long(r['id'])
            entry['Path']=r['path']
            if not r['algo'] == None:
                entry['Algo'] = int(r['algo'])
            else:
                entry['Algo'] = None
            if not r['in_dbs'] == None:
                entry['AlgoInDBS'] = int(r['in_dbs'])
            else:
                entry['AlgoInDBS'] = None
            path = r['path']
            entry['PrimaryDataset']   = path.split('/')[1]
            entry['ProcessedDataset'] = path.split('/')[2]
            entry['DataTier']         = path.split('/')[3]
            ret.append(entry)
        return ret
 
    def execute(self, conn=None, transaction = False):
        binds = self.getBinds()
        result = self.dbi.processData(self.sql, binds, 
                                      conn = conn, transaction = transaction)

        return self.makeDS(self.formatDict(result))

