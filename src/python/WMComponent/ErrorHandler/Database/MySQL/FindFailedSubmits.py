#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_FindFailedCreates_

This module implements the mysql backend for the 
errorhandler, for locating the failed job creation

"""

import threading

from WMCore.Database.DBFormatter import DBFormatter

class FindFailedSubmits(FindFailed):
    """
    This module implements the mysql backend for the 
    submit job error handler.
    
    """

    def execute(self, conn=None, transaction = False):
	jobStatus = 'submitfailed'
	result FindFailed.execute(self.sql, binds,
                         conn = conn, transaction = transaction)

