#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFilesByLimit.py,v 1.1 2010/02/25 22:33:05 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesByLimit \
     import GetAvailableFilesByLimit as GetAvailableFilesByLimitMySQL

class GetAvailableFilesByLimit(GetAvailableFilesByLimitMySQL):
    """
    the same as MySQL implementation
    """