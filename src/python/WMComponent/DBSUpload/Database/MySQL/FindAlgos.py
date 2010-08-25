#!/usr/bin/env python
"""
_DBSUpload.FindAlgos_

Find algos in datasets

"""
__revision__ = "$Id: FindAlgos.py,v 1.5 2009/07/20 18:02:53 mnorman Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "anzar@fnal.gov"

import threading
from WMCore.Database.DBFormatter import DBFormatter

class FindAlgos(DBFormatter):
    
    old_sql = """SELECT A.ID as ID, 
                A.aPP_Name as ApplicationName, 
                A.App_Ver as ApplicationVersion, 
                A.App_Fam as ApplicationFamily, 
                A.PSet_Hash as PSetHash,
                A.Config_Content as PSetContent,
                A.in_dbs as InDBS
                FROM 
                dbsbuffer_algo A 
                    LEFT OUTER JOIN dbsbuffer_dataset D
                     ON D.Algo=A.ID
                     Where D.ID=:dataset"""


    sql = """SELECT A.ID as ID, 
                A.aPP_Name as ApplicationName, 
                A.App_Ver as ApplicationVersion, 
                A.App_Fam as ApplicationFamily, 
                A.PSet_Hash as PSetHash,
                A.Config_Content as PSetContent,
                B.in_dbs as InDBS
                FROM 
                dbsbuffer_algo_dataset_assoc B
                INNER JOIN
                dbsbuffer_algo A
                     ON B.algo_id = A.ID
                WHERE B.dataset_id =:dataset


    """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
    
    def getBinds(self, dataset):
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
		entry['InDBS']=r['indbs']
		ret.append(entry)
	return ret

    def execute(self, datasetInfo=None, conn=None, transaction = False):
        binds = self.getBinds(datasetInfo)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)

        return self.makeAlgo(self.formatDict(result))
    
