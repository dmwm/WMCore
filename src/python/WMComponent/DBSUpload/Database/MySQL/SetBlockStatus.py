#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information

                                                                                                                                                                                                                                                                                                                                                                                                          """
__revision__ = "$Id: SetBlockStatus.py,v 1.1 2009/08/12 22:15:09 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions

from WMCore.Database.DBFormatter import DBFormatter


class SetBlockStatus(DBFormatter):
    sql = \
    """INSERT INTO dbsbuffer_block(blockname, location) VALUES (:block,
       (SELECT ID FROM dbsbuffer_location WHERE se_name =:location))"""



    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)

        
    def execute(self, block, locations = None, conn=None, transaction = False):

        #How this works:
        #Insert an entry into dbsbuffer_block for every location
        #Unique ID that's not the block name

        for location in locations:
            try:
                result = self.dbi.processData(self.sql, {'block': block, 'location': location}, conn = conn, transaction = transaction)
            except Exception, ex:
                raise ex
                                             

