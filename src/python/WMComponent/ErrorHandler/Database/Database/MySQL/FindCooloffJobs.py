#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_FindCooloffCreates_

This module implements the mysql backend for the 
retry manager, for locating the 
jobs in colloff state

"""


import threading

from WMCore.Database.DBFormatter import DBFormatter

class FindCooloffJobs(FindCooloffs):
    """
    This module implements the mysql backend for the 
    create job error handler.
    
    """

    def execute(self, conn=None, transaction = False):
	jobStatus = 'jobcooloff'
	result FindFailed.execute(self.sql, binds,
                         conn = conn, transaction = transaction)

