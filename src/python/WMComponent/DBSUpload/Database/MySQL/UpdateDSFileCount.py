#!/usr/bin/env python
"""
_DBSBuffer.NewFile_

Update UnMigrated File Count in DBS Buffer

"""
__revision__ = "$Id: UpdateDSFileCount.py,v 1.2 2009/01/14 22:06:57 afaq Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
import exceptions

from WMCore.Database.DBFormatter import DBFormatter

#TODO:
# base64 encoding the Run/Lumi INFO, may come up with a beter way in future
#base64.binascii.b2a_base64(str(file.getLumiSections()))
#base64.decodestring('')

class UpdateDSFileCount(DBFormatter):

    sql = """UPDATE dbsbuffer_dataset as A
                   inner join (
                      select * from dbsbuffer_dataset
                          where Path=:path
                   ) as B on A.ID = B.ID
                SET A.UnMigratedFiles = (select count(*) from dbsbuffer_file f where f.dataset = B.ID AND f.status = 'NOTUPLOADED')"""


    #sqlUpdateDS = """UPDATE dbsbuffer_dataset SET UnMigratedFiles = UnMigratedFiles + 1 WHERE ID = (select ID from dbsbuffer_dataset where Path=:path)"""
    def __init__(self):
            myThread = threading.currentThread()
            DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        
    def getBinds(self, dataset=None):
            # binds a list of dictionaries
           binds =  { 
            'path': dataset['Path'],
            }
           
           return binds
       
    def format(self, result):
        return True

    def execute(self, dataset=None, conn=None, transaction = False):
        
        binds = self.getBinds(dataset)
        try:
            result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)

        except Exception, ex:
                raise ex
            
