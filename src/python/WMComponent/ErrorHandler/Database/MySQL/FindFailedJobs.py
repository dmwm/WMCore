#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_FindFailedCreates_

This module implements the mysql backend for the 
errorhandler, for locating the failed job after they were run
basically in JobFailed status

"""

__revision__ = \
    "$Id: FindFailedJobs.py,v 1.1 2009/05/08 16:32:40 afaq Exp $"
__version__ = \
    "$Revision: 1.1 $"
__author__ = \
    "anzar@fnal.gov"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class FindFailedJobs(FindFailed):
    """
    This module implements the mysql backend for the 
    create job error handler.
    
    """

    def execute(self, conn=None, transaction = False):
	jobStatus = 'jobfailed'
	result FindFailed.execute(self.sql, binds,
                         conn = conn, transaction = transaction)

