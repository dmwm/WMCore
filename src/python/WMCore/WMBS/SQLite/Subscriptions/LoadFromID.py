#!/usr/bin/env python
"""
_LoadFromID_

SQLite implementation of Subscription.LoadFromID
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.1 2009/01/14 16:35:24 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.LoadFromID import LoadFromID as LoadFromIDMySQL

class LoadFromID(LoadFromIDMySQL):
    sql = LoadFromIDMySQL.sql
