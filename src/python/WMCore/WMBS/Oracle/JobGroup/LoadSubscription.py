#!/usr/bin/env python
"""
_LoadSubscription_

Oracle implementation of JobGroup.LoadSubscription
"""

__all__ = []
__revision__ = "$Id: LoadSubscription.py,v 1.2 2008/12/05 21:06:26 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.JobGroup.LoadSubscription import LoadSubscription \
    as LoadSubscriptionJobGroupMySQL

class LoadSubscription(LoadSubscriptionJobGroupMySQL):
    sql = LoadSubscriptionJobGroupMySQL.sql