#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_FindCooloffSubmits_

This module implements the mysql backend for the retry manager, for locating the jobs in cooloff state state

"""

__revision__ = \
    "$Id: FindCooloffSubmits.py,v 1.1 2009/05/12 16:39:45 afaq Exp $"
__version__ = \
    "$Revision: 1.1 $"
__author__ = \
    "anzar@fnal.gov"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class FindCooloffSubmits(FindCooloffs):
    """
    This module implements the mysql backend for the 
    finding jobs in submitcooloff state
    
    """

    def execute(self, conn=None, transaction = False):
	jobStatus = 'submitcooloff'
	result FindFailed.execute(self.sql, binds,
                         conn = conn, transaction = transaction)

