#!/usr/bin/env python
"""
_DBSBuffer.NewDataset_

Add a new dataset to DBS Buffer

"""
__revision__ = "$Id: NewDataset.py,v 1.7 2008/12/11 20:32:03 afaq Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "anzar@fnal.gov"

import threading
from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy.exceptions import IntegrityError
import exceptions

class NewDataset(DBFormatter):
        
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        
    sql = """INSERT INTO dbsbuffer_dataset (Path, Algo, AlgoInDBS)
                values (:path, 
                (select ID from dbsbuffer_algo 
                where AppName=:exeName 
                    and AppVer=:appVersion 
                    and AppFam=:appFamily 
                    and PSetHash=:psetHash),
                :algoIn)"""
                
    def getBinds(self, datasetInfo=None, algoInDBS=None):
        # binds a list of dictionaries
        exeName = datasetInfo['ApplicationName']
        appVersion = datasetInfo['ApplicationVersion']
        appFamily = datasetInfo["ApplicationFamily"]
        psetHash = datasetInfo.get('PSetHash',None)
        if psetHash == None:
            psetHash = "NO_PSET_HASH"
        else:
            if psetHash.find(";"):
                # no need for fake hash in new schema
                psetHash = psetHash.split(";")[0]
                psetHash = psetHash.replace("hash=", "")
                
        binds =  { 
                        'path': "/"+datasetInfo['PrimaryDataset']+ \
                                "/"+datasetInfo['ProcessedDataset']+ \
                                "/"+datasetInfo['DataTier'],
                        'exeName' : exeName,
                        'appVersion' : appVersion,
                        'appFamily' : appFamily,
                        'psetHash' : psetHash,
                        'algoIn' : algoInDBS
                }
        return binds

    def format(self, result):
        return True

    def execute(self, dataset=None, algoInDBS=0, conn=None, transaction = False):
        binds = self.getBinds(dataset, algoInDBS)

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

