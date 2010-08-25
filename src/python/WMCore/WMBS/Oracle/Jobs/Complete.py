#!/usr/bin/env python
"""
_Complete_

Oracle implementation of Jobs.Complete
"""

__all__ = []
__revision__ = "$Id: Complete.py,v 1.5 2009/03/20 14:29:17 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Jobs.Complete import Complete as CompleteJobMySQL

class Complete(CompleteJobMySQL):
    insertSQL = CompleteJobMySQL.insertSQL
