#!/usr/bin/env python
"""
_ClearStatus_
Oracle implementation of Jobs.ClearStatus

Delete all status information. For resubmissions and for each state change.
"""
__all__ = []
__revision__ = "$Id: ClearStatus.py,v 1.2 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.ClearStatus import ClearStatus as ClearStatusJobMySQL

class ClearStatus(ClearStatusJobMySQL):
    
    sql = ClearStatusJobMySQL.sql