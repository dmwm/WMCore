#!/usr/bin/env python
"""
_GetAvailableFilesMeta_

Oracle implementation of Subscription.GetAvailableFilesMeta
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesMeta import \
     GetAvailableFilesMeta as GetAvailableFilesMetaMySQL

class GetAvailableFilesMeta(GetAvailableFilesMetaMySQL):
    pass
