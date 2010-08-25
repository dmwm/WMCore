#!/usr/bin/env python
"""
_GetAvailableFiles_

Oracle implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetAcquiredFilesByLimit import \
     GetAcquiredFilesByLimit as GetAcquiredFilesByLimitMySQL

class GetAcquiredFilesByLimit(GetAcquiredFilesByLimitMySQL):
    """
    same as mysql version
    """