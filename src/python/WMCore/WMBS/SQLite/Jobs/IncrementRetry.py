#!/usr/bin/env python
"""
_IncrementRetry_

SQLite implementation of Jobs.IncrementRetry
"""

__revision__ = "$Id: IncrementRetry.py,v 1.1 2010/07/13 22:11:01 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.IncrementRetry import IncrementRetry as IncrementRetryMySQL

class IncrementRetry(IncrementRetryMySQL):
    pass
