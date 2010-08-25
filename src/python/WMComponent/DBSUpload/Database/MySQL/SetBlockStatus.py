#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information

                                                                                                                                                                                                                                                                                                                                                                                                          """
__revision__ = "$Id: SetBlockStatus.py,v 1.3 2009/09/03 16:55:27 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions

from WMCore.Database.DBFormatter import DBFormatter


class SetBlockStatus(DBFormatter):
    sql = """INSERT INTO dbsbuffer_block (blockname, location) 
               SELECT :block, id AS location FROM dbsbuffer_location
               WHERE se_name = :location AND NOT EXISTS
                 (SELECT * FROM dbsbuffer_block WHERE blockname = :block AND
                    location = (SELECT id FROM dbsbuffer_location WHERE se_name = :location))"""

    def execute(self, block, locations = None, conn = None, transaction = False):
        """
        _execute_

        Given a block and a list of locations add entries to the dbsbuffer_block
        table.
        """
        bindVars = []

        for location in locations:
            bindVars.append({"block": block, "location": location})

        self.dbi.processData(self.sql, bindVars, conn = conn,
                             transaction = transaction)
        return
                                             

