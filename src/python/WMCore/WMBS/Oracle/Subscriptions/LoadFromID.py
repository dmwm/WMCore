#!/usr/bin/env python
"""
_LoadFromID_

Oracle implementation of Subscription.LoadFromID
"""

__revision__ = "$Id: LoadFromID.py,v 1.3 2009/10/12 21:11:12 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.LoadFromID import LoadFromID as LoadFromIDMySQL

class LoadFromID(LoadFromIDMySQL):
    pass
