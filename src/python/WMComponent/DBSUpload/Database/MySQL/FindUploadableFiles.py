#!/usr/bin/env python
"""
_DBSUpload.FindUploadableFiles_

Find the files in a datasets that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableFiles.py,v 1.5 2008/12/30 17:47:33 afaq Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
from WMCore.Database.DBFormatter import DBFormatter

class FindUploadableFiles(DBFormatter):
    
    sql = """SELECT wmbsfile.id as ID,
		wmbsfile.lfn as LFN, 
		wmbsfile.size as FileSize, 
		wmbsfile.events as TotalEvents,
		wmbsfile.cksum as Checksum
		FROM dbsbuffer_file buffile 
			join wmbs_file_details wmbsfile 
				on wmbsfile.id=buffile.id 
		where buffile.dataset=:dataset and buffile.FileStatus =:status LIMIT 10"""

    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
    
    def getBinds(self, dataset):
        binds =  { 'dataset': dataset['ID'], 'status':'NOTUPLOADED' }
        return binds

    def makeFile(self, results):
        ret=[]
        for r in results:
                entry={}
                entry['ID']=long(r['id'])
                entry['LFN']=r['lfn']
                entry['FileSize']=r['filesize']
                entry['TotalEvents']=r['totalevents']
                entry['Checksum']=r['checksum']
                ret.append(entry)
        return ret

    def execute(self, datasetInfo=None, conn=None, transaction = False):
        binds = self.getBinds(datasetInfo)
        print "SQL: %s" %(self.sql)
        print "BINDS: %s" %str(binds)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.makeFile(self.formatDict(result))
    
