#!/usr/bin/env python
"""
_GetAvailableFilesMeta_

SQLite implementation of Subscription.GetAvailableFilesMeta

"""




from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesMeta import GetAvailableFilesMeta \
     as GetAvailableFilesMetaMySQL

class GetAvailableFilesMeta(GetAvailableFilesMetaMySQL):
    pass
