#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information

                                                                                                                                                                                                                                                                                                                                                                                                          """
__revision__ = "$Id: SetBlocksInjected.py,v 1.2 2009/08/24 09:44:27 meloam Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions

from WMCore.Database.DBFormatter import DBFormatter


class SetBlocksInjected(DBFormatter):
    sql = \
    """UPDATE dbsbuffer_block 
        SET is_in_phedex = 1 
        WHERE  blockname = :block
          AND  location = (SELECT ID FROM dbsbuffer_location WHERE se_name =:location)  
    """
        
    def execute(self, blocksLocations, conn=None, transaction = False):

        #How this works:
        #Insert an entry into dbsbuffer_block for every location
        #Unique ID that's not the block name

        for row in blocksLocations:
            result = self.dbi.processData(self.sql, {'block': row['blockname'], 'location': row['location']}, conn = conn, transaction = transaction)
            
                                             

