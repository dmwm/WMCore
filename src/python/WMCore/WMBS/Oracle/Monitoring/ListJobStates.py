#!/usr/bin/env python
"""
_ListJobStates_

Oracle implementation of Monitoring.ListJobStates
"""

__revision__ = "$Id: ListJobStates.py,v 1.1 2010/01/25 20:42:37 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Monitoring.ListJobStates import ListJobStates \
    as ListJobStatesMySQL

class ListJobStates(ListJobStatesMySQL):
    pass
