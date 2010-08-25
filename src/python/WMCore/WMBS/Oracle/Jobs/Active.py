#!/usr/bin/env python
"""
_Active_

Oracle implementation of Jobs.Active
"""

__all__ = []
__revision__ = "$Id: Active.py,v 1.5 2009/03/20 14:29:17 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Jobs.Active import Active as ActiveJobMySQL

class Active(ActiveJobMySQL):
    insertSQL = ActiveJobMySQL.insertSQL
