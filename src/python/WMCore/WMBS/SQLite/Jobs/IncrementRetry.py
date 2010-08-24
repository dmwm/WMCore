#!/usr/bin/env python
"""
_IncrementRetry_

SQLite implementation of Jobs.IncrementRetry
"""




from WMCore.WMBS.MySQL.Jobs.IncrementRetry import IncrementRetry as IncrementRetryMySQL

class IncrementRetry(IncrementRetryMySQL):
    pass
