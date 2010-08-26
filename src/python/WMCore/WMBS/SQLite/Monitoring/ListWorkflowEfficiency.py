#!/usr/bin/env python
"""
_ListWorkflowEfficiency_

SQLite implementation of Monitoring.ListWorkflowEfficiency
"""

__revision__ = "$Id: ListWorkflowEfficiency.py,v 1.1 2010/01/26 21:37:19 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Monitoring.ListWorkflowEfficiency import ListWorkflowEfficiency \
    as ListWorkflowEfficiencyMySQL

class ListWorkflowEfficiency(ListWorkflowEfficiencyMySQL):
    pass
