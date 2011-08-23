#!/usr/bin/env python
"""
_AutoIncrementCheck_

AutoIncrement Check
Test to properly set the autoIncrement value

First, find the highest jobID either in wmbs_job or in wmbs_highest_job
Then reset AUTO_INCREMENT to point to that.
"""

__all__ = []

import logging

from WMCore.Database.DBFormatter import DBFormatter

class AutoIncrementCheck(DBFormatter):
    """
    _AutoIncrmentCheck_

    Check and properly set the auto_increment counter for wmbs_job
    """


    highestSQL = """SELECT IFNULL(MAX(id), 0) FROM wmbs_job"""

    currentSQL = """SELECT Auto_increment FROM information_schema.tables WHERE table_name='wmbs_job' AND table_schema=DATABASE()"""

    alterSQL = "ALTER TABLE wmbs_job AUTO_INCREMENT = :value"


    def execute(self, input = 0, conn = None, transaction = False):
        """
        _execute_


        """

        highest = self.dbi.processData(self.highestSQL, {}, conn = conn,
                                       transaction = transaction)[0].fetchall()[0][0]

        current = self.dbi.processData(self.currentSQL, {}, conn = conn,
                                       transaction = transaction)[0].fetchall()[0][0]

        value = max(input + 1, highest + 1)

        if value > current:
            self.dbi.processData(self.alterSQL, {'value': value},
                                 conn = conn, transaction = transaction)

        return
