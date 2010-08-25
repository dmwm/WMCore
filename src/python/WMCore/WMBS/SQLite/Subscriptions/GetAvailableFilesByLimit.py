#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesByLimit \
     import GetAvailableFilesByLimit as GetAvailableFilesByLimitMySQL

class GetAvailableFilesByLimit(GetAvailableFilesByLimitMySQL):
    """
    the same as MySQL implementation
    """