#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_FindCooloffSubmits_

This module implements the mysql backend for the retry manager, for locating the jobs in cooloff state state

"""


    "$Id: FindCooloffSubmits.py,v 1.2 2010/08/18 15:38:36 meloam Exp $"

    "$Revision: 1.2 $"

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

