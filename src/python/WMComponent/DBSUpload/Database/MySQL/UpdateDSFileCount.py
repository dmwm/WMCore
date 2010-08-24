#!/usr/bin/env python
"""
_DBSBuffer.NewFile_

Update UnMigrated File Count in DBS Buffer

"""
__revision__ = "$Id: UpdateDSFileCount.py,v 1.1 2008/11/18 23:25:29 afaq Exp $"
__version__ = "$Revision: 1.1 $"
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
                SET A.UnMigratedFiles = A.UnMigratedFiles - :cnt"""

    #sqlUpdateDS = """UPDATE dbsbuffer_dataset SET UnMigratedFiles = UnMigratedFiles + 1 WHERE ID = (select ID from dbsbuffer_dataset where Path=:path)"""
    def __init__(self):
            myThread = threading.currentThread()
            DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        
    def getBinds(self, dataset=None, count=0):
            # binds a list of dictionaries
           binds =  { 
            'path': dataset['Path'],
            'cnt' : count
            }
           
           return binds
       
    def format(self, result):
        return True

    def execute(self, dataset=None, count =0, conn=None, transaction = False):
        
        binds = self.getBinds(dataset, count)

        try:
            result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)

        except Exception, ex:
                raise ex
            