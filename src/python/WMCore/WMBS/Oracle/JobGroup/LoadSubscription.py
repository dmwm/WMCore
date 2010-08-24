#!/usr/bin/env python
"""
_LoadSubscription_

MySQL implementation of JobGroup.LoadSubscription
"""

__all__ = []
__revision__ = "$Id: LoadSubscription.py,v 1.1 2008/11/24 21:51:45 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.LoadSubscription import LoadSubscription \
    as LoadSubscriptionJobGroupMySQL

class LoadSubscription(LoadSubscriptionJobGroupMySQL):
    sql = LoadSubscriptionJobGroupMySQL.sql