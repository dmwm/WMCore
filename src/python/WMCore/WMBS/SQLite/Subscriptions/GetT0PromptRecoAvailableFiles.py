#!/usr/bin/env python
"""
_GetT0PromptRecoAvailableFiles_

SQLite implementation of Subscription.GetT0PromptRecoAvailableFiles
"""

__revision__ = "$Id: GetT0PromptRecoAvailableFiles.py,v 1.2 2009/10/27 09:03:43 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.GetT0PromptRecoAvailableFiles import GetT0PromptRecoAvailableFiles \
     as GetT0PromptRecoAvailableFilesMySQL

class GetT0PromptRecoAvailableFiles(GetT0PromptRecoAvailableFilesMySQL):
    pass

