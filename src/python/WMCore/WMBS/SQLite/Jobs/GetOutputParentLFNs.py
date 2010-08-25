#!/usr/bin/env python
"""
_GetOutputParentLFNs

SQLite implementation of Jobs.GetOutputParentLFNs
"""

__revision__ = "$Id: GetOutputParentLFNs.py,v 1.1 2009/08/21 10:47:03 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.GetOutputParentLFNs import GetOutputParentLFNs as GetOutputParentLFNsMySQL

class GetOutputParentLFNs(GetOutputParentLFNsMySQL):
    pass
