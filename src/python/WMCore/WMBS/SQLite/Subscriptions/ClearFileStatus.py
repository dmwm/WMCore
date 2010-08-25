#!/usr/bin/env python
"""
_ClearFileStatus_
SQLite implementation of Subscriptions.ClearFileStatus

Delete all file status information. For resubmissions and for each state change.
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.ClearFileStatus import ClearFileStatus \
     as ClearFileStatusMySQL

class ClearFileStatus(ClearFileStatusMySQL):
    sql = ClearFileStatusMySQL.sql
