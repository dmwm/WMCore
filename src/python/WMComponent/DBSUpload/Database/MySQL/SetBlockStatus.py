#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information

                                                                                                                                                                                                                                                                                                                                                                                                          """
__revision__ = "$Id: SetBlockStatus.py,v 1.7 2009/09/23 16:39:19 mnorman Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions

from sets import Set

from WMCore.Database.DBFormatter import DBFormatter


class SetBlockStatus(DBFormatter):

    sql = """INSERT INTO dbsbuffer_block (blockname, location, open_status)
               SELECT :block, (SELECT id FROM dbsbuffer_location WHERE se_name = :location), :open_status FROM DUAL
               WHERE NOT EXISTS (SELECT blockname FROM dbsbuffer_block WHERE blockname = :block
               and location = (SELECT id FROM dbsbuffer_location WHERE se_name = :location))
    """


    def __init__(self, logger = None, dbi = None):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)


    def execute(self, block, locations = None, open_status = 0, conn = None, transaction = False):
        """
        _execute_

        Given a block and a list of locations add entries to the dbsbuffer_block
        table.
        """
        bindVars = []

        locations = list(Set(locations))

        for location in locations:
            bindVars.append({"block": block, "location": location, "open_status": open_status})

        self.dbi.processData(self.sql, bindVars, conn = conn,
                             transaction = transaction)
        return
                                             

