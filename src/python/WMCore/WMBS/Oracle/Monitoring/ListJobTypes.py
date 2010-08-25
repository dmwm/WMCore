#!/usr/bin/env python
"""
_ListJobTypes_

Oracle implementation of Monitoring.ListJobTypes
"""

__revision__ = "$Id: ListJobTypes.py,v 1.1 2010/01/25 20:47:43 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Monitoring.ListJobTypes import ListJobTypes \
    as ListJobTypesMySQL

class ListJobTypes(ListJobTypesMySQL):
    pass
