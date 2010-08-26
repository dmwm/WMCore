#!/usr/bin/env python
"""
_ListIncomplete_

SQLite implementation of Subscription.ListIncomplete
"""

__all__ = []
__revision__ = "$Id: ListIncomplete.py,v 1.1 2009/07/07 18:27:25 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.ListIncomplete import ListIncomplete as ListIncompleteMySQL

class ListIncomplete(ListIncompleteMySQL):
    pass
