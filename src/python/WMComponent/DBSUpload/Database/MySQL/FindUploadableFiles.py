#!/usr/bin/env python
"""
_DBSUpload.FindUploadableFiles_

Find the files in a datasets that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableFiles.py,v 1.8 2009/06/04 21:45:17 mnorman Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
from WMCore.Database.DBFormatter import DBFormatter

class FindUploadableFiles(DBFormatter):
    
    sqlOld = """SELECT file.id as ID,
		file.lfn as LFN, 
		file.size as FileSize, 
		file.events as TotalEvents,
		file.cksum as Checksum
		FROM dbsbuffer_file file 
			where file.dataset=:dataset and file.status =:status LIMIT 10"""

    sql = """SELECT dbsfile.id as ID FROM dbsbuffer_file dbsfile where dbsfile.dataset=:dataset and dbsfile.status =:status LIMIT :maxfiles""" 

    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
    
    def getBinds(self, dataset, maxfiles):
        binds =  { 'dataset': dataset['ID'], 'status':'NOTUPLOADED', 'maxfiles': maxfiles }
        return binds

    def makeFile(self, results):
        ret=[]
        for r in results:
                entry={}
                entry['ID']=long(r['id'])
                #entry['LFN']=r['lfn']
                #entry['FileSize']=r['filesize']
                #entry['TotalEvents']=r['totalevents']
                #entry['Checksum']=r['checksum']
                ret.append(entry)
        return ret

    def execute(self, datasetInfo=None, maxfiles=10, conn=None, transaction = False):

        print datasetInfo
        binds = self.getBinds(datasetInfo, maxfiles)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.makeFile(self.formatDict(result))
    
