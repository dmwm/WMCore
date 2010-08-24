#!/usr/bin/env python
"""
_DBSBuffer.NewAlgo_

Add a new algorithm to DBS Buffer

"""
__revision__ = "$Id: NewAlgo.py,v 1.3 2009/01/14 22:07:25 afaq Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "anzar@fnal.gov"

import threading
from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy.exceptions import IntegrityError
import exceptions

class NewAlgo(DBFormatter):
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)

    sql = """INSERT INTO dbsbuffer_algo (AppName, AppVer, AppFam, PSetHash, ConfigContent)
                values (:exeName, :appVersion, :appFamily, :psetHash, :psetContent)"""

    def getBinds(self, datasetInfo=None):
        # binds a list of dictionaries
        exeName = datasetInfo['ApplicationName']
        appVersion = datasetInfo['ApplicationVersion']
        appFamily = datasetInfo["ApplicationFamily"]
        psetContent = datasetInfo.get('PSetContent',None)

	print "TEMPORARY FOR TESTING........"
	psetContent="THIS IS A TEST CONTENT"

        if psetContent == None:
            psetContent = "PSET_CONTENT_NOT_AVAILABLE"
        psetHash = datasetInfo.get('PSetHash',None)
        if psetHash == None:
            psetHash = "NO_PSET_HASH"
        else:
            if psetHash.find(";"):
                # no need for fake hash in new schema
                psetHash = psetHash.split(";")[0]
                psetHash = psetHash.replace("hash=", "")
        binds =  { 
                  'exeName' : exeName,
                  'appVersion' : appVersion,
                  'appFamily' : appFamily,
                  'psetContent' : str(psetContent),
                  'psetHash' : psetHash
                  }
        return binds

    def format(self, result):
        return True

    def execute(self, dataset=None, conn=None, transaction = False):
        binds = self.getBinds(dataset)

        try:
                        
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
        except Exception, ex:
            if ex.__str__().find("Duplicate entry") != -1 :
                #print "DUPLICATE: so what !!"
                return
            else:
                raise ex
        return 
        #return self.format(result)
