#!/usr/bin/env python
"""
_DBSUpload.FindUploadableDatasets_

Find the datasets that have files that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableDatasets.py,v 1.4 2008/12/17 21:57:10 afaq Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
from WMCore.Database.DBFormatter import DBFormatter

class FindUploadableDatasets(DBFormatter):
    
    sql = """SELECT ds.id as ID, ds.Path as Path, ds.Algo as Algo, ds.AlgoInDBS as AlgoInDBS FROM dbsbuffer_dataset ds WHERE UnMigratedFiles >= 10 """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)

    def makeDS(self, results):
        ret=[]
        for r in results:
                entry={}
                entry['ID']=long(r['id'])
                entry['Path']=r['path']
                entry['Algo']=int(r['algo'])
                entry['AlgoInDBS']=int(r['algoindbs'])
                ret.append(entry)
        return ret
 
    def execute(self, conn=None, transaction = False):
        binds = self.getBinds()
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.makeDS(self.formatDict(result))

