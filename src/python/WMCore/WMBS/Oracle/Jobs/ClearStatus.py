#!/usr/bin/env python
"""
_ClearStatus_
MySQL implementation of Jobs.ClearStatus

Delete all status information. For resubmissions and for each state change.
"""
__all__ = []
__revision__ = "$Id: ClearStatus.py,v 1.1 2008/11/24 21:51:40 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.ClearStatus import ClearStatus as ClearStatusJobMySQL

class ClearStatus(learStatucJobMySQL):
    
    sql = learStatucJobMySQL.sql