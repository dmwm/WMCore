#!/usr/bin/env python
"""
_GetParentInfo_

Oracle implementation of Files.GetParentInfo
"""

__revision__ = "$Id: GetParentInfo.py,v 1.3 2010/06/28 19:06:36 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Files.GetParentInfo import GetParentInfo as GetParentInfoMySQL

class GetParentInfo(GetParentInfoMySQL):
    pass
