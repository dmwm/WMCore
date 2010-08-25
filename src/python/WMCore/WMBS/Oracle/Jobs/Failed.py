#!/usr/bin/env python
"""
_Failed_

Oracle implementation of Jobs.Failed
"""

__all__ = []
__revision__ = "$Id: Failed.py,v 1.5 2009/03/20 14:29:17 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Jobs.Failed import Failed as FailedJobMySQL

class Failed(FailedJobMySQL):
    insertSQL = FailedJobMySQL.insertSQL
