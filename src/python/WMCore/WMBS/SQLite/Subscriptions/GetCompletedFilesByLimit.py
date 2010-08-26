#!/usr/bin/env python
"""
_GetAvailableFiles_

Oracle implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetCompletedFilesByLimit.py,v 1.1 2010/02/26 20:20:36 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFilesByLimit import \
     GetCompletedFilesByLimit as GetCompletedFilesByLimitMySQL

class GetCompletedFilesByLimit(GetCompletedFilesByLimitMySQL):
    """
    same as mysql version
    """