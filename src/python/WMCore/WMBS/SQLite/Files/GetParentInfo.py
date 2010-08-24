#!/usr/bin/env python
"""
_GetParentInfo_

SQLite implementation of Files.GetParentInfo
"""




from WMCore.WMBS.MySQL.Files.GetParentInfo import GetParentInfo as GetParentInfoMySQL

class GetParentInfo(GetParentInfoMySQL):
    pass
