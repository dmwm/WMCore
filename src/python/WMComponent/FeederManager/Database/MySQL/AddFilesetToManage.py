#!/usr/bin/env python
"""
_AddFilesetToManage_

MySQL implementation of FeederManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter
import time

class AddFilesetToManage(DBFormatter):

    sql = """
INSERT INTO managed_filesets(fileset, feeder, insert_time)
VALUES (:id, :type, :time)
            """

    def getBinds(self, fileset = '', feederType = ''):
        """
        Bind parameters
        """
        dict = {'id' : fileset,
                'type': feederType,
                'time' : int(time.time())}

        return dict

    def execute(self, fileset = '', feederType = '', conn = None, transaction = False):
        """
        Add fileset to manage
        """
        binds = self.getBinds(fileset, feederType)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
