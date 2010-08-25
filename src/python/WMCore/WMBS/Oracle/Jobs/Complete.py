#!/usr/bin/env python
"""
_Complete_

Oracle implementation of Jobs.Complete
"""

__all__ = []
__revision__ = "$Id: Complete.py,v 1.6 2009/04/27 21:12:33 sryu Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.MySQL.Jobs.Complete import Complete as CompleteJobMySQL

class Complete(CompleteJobMySQL):
    insertSQL = CompleteJobMySQL.insertSQL
    updateSQL = CompleteJobMySQL.updateSQL