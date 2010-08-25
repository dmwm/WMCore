#!/usr/bin/env python
"""
_GetMergedChildren_

SQLite implementation of Files.GetMergedChildren
"""

__revision__ = "$Id: GetMergedChildren.py,v 1.1 2009/12/18 17:54:08 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.GetMergedChildren import GetMergedChildren as GetMergedChildrenMySQL

class GetMergedChildren(GetMergedChildrenMySQL):
    pass
