#!/usr/bin/env python
"""
_GetAvailableFilesMeta_

SQLite implementation of Subscription.GetAvailableFilesMeta

"""

__revision__ = "$Id: GetAvailableFilesMeta.py,v 1.1 2009/07/23 20:51:36 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesMeta import GetAvailableFilesMeta \
     as GetAvailableFilesMetaMySQL

class GetAvailableFilesMeta(GetAvailableFilesMetaMySQL):
    pass
