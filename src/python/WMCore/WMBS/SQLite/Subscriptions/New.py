#!/usr/bin/env python
"""
_Subscription.New_

SQLite implementation of Subscription.New
"""

__revision__ = "$Id: New.py,v 1.6 2009/10/12 21:11:11 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.MySQL.Subscriptions.New import New as NewMySQL

class New(NewMySQL):
    """
    Create a workflow ready for subscriptions
    """
    pass
