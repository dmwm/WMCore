#!/usr/bin/env python
"""
_ClearFileStatus_
SQLite implementation of Subscriptions.ClearFileStatus

Delete all file status information. For resubmissions and for each state change.
"""

__all__ = []
__revision__ = "$Id: ClearFileStatus.py,v 1.1 2009/04/08 18:55:29 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.ClearFileStatus import ClearFileStatus \
     as ClearFileStatusMySQL

class ClearFileStatus(ClearFileStatusMySQL):
    sql = ClearFileStatusMySQL.sql
