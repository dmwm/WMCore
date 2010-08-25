#!/usr/bin/env python
"""
_Active_

Oracle implementation of Jobs.Active
"""

__all__ = []
__revision__ = "$Id: Active.py,v 1.6 2009/04/10 15:42:48 sryu Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.MySQL.Jobs.Active import Active as ActiveJobsMySQL

class Active(ActiveJobsMySQL):
    insertSQL = ActiveJobsMySQL.insertSQL
    updateSQL = ActiveJobsMySQL.updateSQL