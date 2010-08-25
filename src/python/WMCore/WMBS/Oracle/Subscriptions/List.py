#!/usr/bin/env python
"""
_List_

Oracle implementation of Subscription.List
"""

__all__ = []
__revision__ = "$Id: List.py,v 1.1 2009/03/10 12:54:31 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.List import List as ListMySQL

class List(ListMySQL):
    sql =  ListMySQL.sql
