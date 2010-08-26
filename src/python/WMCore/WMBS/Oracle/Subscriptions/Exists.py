#!/usr/bin/env python
"""
_Exists_

Oracle implementation of Subscription.Exists
"""

__revision__ = "$Id: Exists.py,v 1.5 2009/10/12 21:11:12 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Subscriptions.Exists import Exists as ExistsMySQL

class Exists(ExistsMySQL):    
    pass
