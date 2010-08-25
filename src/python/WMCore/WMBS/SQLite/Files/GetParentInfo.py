#!/usr/bin/env python
"""
_GetParentInfo_

SQLite implementation of Files.GetParentInfo
"""

__revision__ = "$Id: GetParentInfo.py,v 1.1 2009/12/21 20:46:53 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.GetParentInfo import GetParentInfo as GetParentInfoMySQL

class GetParentInfo(GetParentInfoMySQL):
    pass
