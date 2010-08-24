#!/usr/bin/env python
"""
_IncrementRetry_

Oracle implementation of Jobs.IncrementRetry
"""




from WMCore.WMBS.MySQL.Jobs.IncrementRetry import IncrementRetry as IncrementRetryMySQL

class IncrementRetry(IncrementRetryMySQL):
    pass
