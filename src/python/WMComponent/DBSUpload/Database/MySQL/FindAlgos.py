#!/usr/bin/env python
"""
_DBSUpload.FindAlgos_

Find algos in datasets

"""
__revision__ = "$Id: FindAlgos.py,v 1.3 2008/11/18 23:25:29 afaq Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "anzar@fnal.gov"

import threading
from WMCore.Database.DBFormatter import DBFormatter

class FindAlgos(DBFormatter):
    
    sql = """SELECT A.ID as ID, 
                A.AppName as ApplicationName, 
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
        binds =  { 'dataset': dataset['ID']}
        return binds

    def execute(self, datasetInfo=None, conn=None, transaction = False):
        binds = self.getBinds(datasetInfo)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
    