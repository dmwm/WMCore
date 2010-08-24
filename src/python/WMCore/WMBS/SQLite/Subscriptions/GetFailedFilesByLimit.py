#!/usr/bin/env python
"""
_GetAvailableFiles_

Oracle implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetFailedFilesByLimit import \
     GetFailedFilesByLimit as GetFailedFilesByLimitMySQL

class GetFailedFilesByLimit(GetFailedFilesByLimitMySQL):
    """
    same as mysql version
    """