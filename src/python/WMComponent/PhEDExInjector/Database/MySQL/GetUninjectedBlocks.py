#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information

                                                                                                                                                                                                                                                                                                                                                                                                          """
__revision__ = "$Id: GetUninjectedBlocks.py,v 1.2 2009/08/24 09:44:27 meloam Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions

from WMCore.Database.DBFormatter import DBFormatter


class GetUninjectedBlocks(DBFormatter):
    sql = \
    """SELECT dbsbuffer_block.blockname as blockname, dbsbuffer_location.se_name as location 
            FROM dbsbuffer_block, dbsbuffer_location 
            WHERE dbsbuffer_block.location = dbsbuffer_location.id
              AND dbsbuffer_block.is_in_phedex = 0
        """
        
    def execute(self, conn=None, transaction = False):

        result = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        return self.formatDict(result)

