#!/usr/bin/env python
"""
_DBSUpload.FindAlgos_

Find algos in datasets

"""
__revision__ = "$Id: FindAlgos.py,v 1.4 2008/12/17 21:57:10 afaq Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "anzar@fnal.gov"

import threading
from WMCore.Database.DBFormatter import DBFormatter

class FindAlgos(DBFormatter):
    
    sql = """SELECT A.ID as ID, 
                A.aPPName as ApplicationName, 
                A.AppVer as ApplicationVersion, 
                A.AppFam as ApplicationFamily, 
                A.PSetHash as PSetHash,
                A.ConfigContent as PSetContent, 
                A.LastModificationDate as LUD
                FROM 
                dbsbuffer_algo A 
                    left outer join dbsbuffer_dataset D
                     on D.Algo=A.ID
                     Where D.ID=:dataset"""
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
    
    def getBinds(self, dataset):
	print dataset
        binds =  { 'dataset': dataset['ID']}
        return binds

    def makeAlgo(self, results):
	ret=[]
	for r in results:
		entry={}
		entry['ApplicationName']=r['applicationname']
		entry['ApplicationVersion']=r['applicationversion']
		entry['ApplicationVersion']=r['applicationversion']
		entry['ApplicationFamily']=r['applicationfamily']
		entry['PSetHash']=r['psethash']
		entry['PSetContent']=r['psetcontent']
		entry['LUD']=r['lud']
		ret.append(entry)
	return ret

    def execute(self, datasetInfo=None, conn=None, transaction = False):
        binds = self.getBinds(datasetInfo)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.makeAlgo(self.formatDict(result))
    
