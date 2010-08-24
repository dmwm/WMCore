#!/usr/bin/env python
"""
_ClearStatus_
Oracle implementation of Jobs.ClearStatus

Delete all status information. For resubmissions and for each state change.
"""

__all__ = []
__revision__ = "$Id: ClearStatus.py,v 1.3 2009/01/12 19:26:04 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Jobs.ClearStatus import ClearStatus as ClearStatusJobMySQL

class ClearStatus(ClearStatusJobMySQL):
    sql = ClearStatusJobMySQL.sql
