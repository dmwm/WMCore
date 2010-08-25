#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information

                                                                                                                                                                                                                                                                                                                                                                                                          """
__revision__ = "$Id: SetBlockStatus.py,v 1.5 2009/09/03 19:00:37 mnorman Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions

from sets import Set

from WMCore.Database.DBFormatter import DBFormatter


class SetBlockStatus(DBFormatter):

    sql = """INSERT INTO dbsbuffer_block (blockname, location)
               SELECT :block, (SELECT id FROM dbsbuffer_location WHERE se_name = :location) FROM DUAL
               WHERE NOT EXISTS (SELECT blockname FROM dbsbuffer_block WHERE blockname = :block
               and location = (SELECT id FROM dbsbuffer_location WHERE se_name = :location))
    """


    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)


    def execute(self, block, locations = None, conn = None, transaction = False):
        """
        _execute_

        Given a block and a list of locations add entries to the dbsbuffer_block
        table.
        """
        bindVars = []

        locations = list(Set(locations))

        for location in locations:
            bindVars.append({"block": block, "location": location})

        self.dbi.processData(self.sql, bindVars, conn = conn,
                             transaction = transaction)
        return
                                             

