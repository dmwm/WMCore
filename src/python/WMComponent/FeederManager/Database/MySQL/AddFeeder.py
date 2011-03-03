#!/usr/bin/env python
"""
_AddFeeder_

MySQL implementation of FeederManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter
import time

class AddFeeder(DBFormatter):

    sql = """INSERT INTO
managed_feeders(feeder_type, feeder_state, insert_time)
VALUES (:type, :state, :time)
            """

    def getBinds(self, feederType = '', feederState = ''):
        """
        Bind parameters
        """
        dict = {'type': feederType,
                'state': feederState,
                'time': int(time.time())}

        return dict

    def execute(self, feederType = '', feederState = '', conn = None, transaction = False):
        """
        Add new feeder
        """
        binds = self.getBinds(feederType, feederState)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
