#!/usr/bin/env python
"""
_DBSBuffer.UpdateAlgo_

Add PSetHash to Algo in DBS Buffer

"""
__revision__ = "$Id: UpdateAlgo.py,v 1.1 2008/11/18 23:25:29 afaq Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
from WMCore.Database.DBFormatter import DBFormatter
import exceptions

class UpdateAlgo(DBFormatter):

    sql = """UPDATE dbsbuffer_algo
                SET PSetHash=:psetHash 
                WHERE
                AppName=:exeName
                AND AppVer=:appVersion
                AND AppFam=:appFamily
                AND ID = 
                    (select Algo FROM dbsbuffer_dataset WHERE Path = :path)
                """
    
    def __init__(self):
            myThread = threading.currentThread()
            DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        
    def getBinds(self, datasetInfo=None, psethash=None):
        # binds a list of dictionaries
        binds =  { 
                  'exeName' : datasetInfo['ApplicationName'],
                  'appVersion' : datasetInfo['ApplicationVersion'],
                  'appFamily' : datasetInfo['ApplicationFamily'],
                  'psetHash' : psethash,
                  'path' :      "/"+datasetInfo['PrimaryDataset']+ \
                                "/"+datasetInfo['ProcessedDataset']+ \
                                "/"+datasetInfo['DataTier']
                  }
        return binds
       
    def format(self, result):
        return True

    def execute(self, dataset=None, psethash=None, conn=None, transaction = False):
        binds = self.getBinds(dataset, psethash)
        try:
            result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        except Exception, ex:
            if ex.__str__().find("Duplicate entry") != -1 :
                #print "DUPLICATE: so what !!"
                return
            else:
                raise ex

        return 
        #return self.format(result)

