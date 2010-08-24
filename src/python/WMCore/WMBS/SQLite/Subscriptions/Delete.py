#!/usr/bin/env python
"""
_Delete_

SQLite implementation of Subscription.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/11/20 22:54:26 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.Delete import Delete as DeleteMySQL

class Delete(DeleteMySQL):
    sql = DeleteMySQL.sql
