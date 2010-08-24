#!/usr/bin/env python
"""
_LoadSubscription_

SQLite implementation of JobGroup.LoadSubscription
"""

__all__ = []
__revision__ = "$Id: LoadSubscription.py,v 1.1 2008/11/21 17:14:58 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.LoadSubscription import LoadSubscription as LoadSubscriptionMySQL

class LoadSubscription(LoadSubscriptionMySQL):
    sql = LoadSubscriptionMySQL.sql
