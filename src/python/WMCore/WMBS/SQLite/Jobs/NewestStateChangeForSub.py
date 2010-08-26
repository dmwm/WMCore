#!/usr/bin/env python
"""
_NewestStateChangeForSub_

SQLite implementation of Jobs.NewestStateChangeForSub
"""

__all__ = []
__revision__ = "$Id: NewestStateChangeForSub.py,v 1.1 2009/08/03 18:39:44 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.NewestStateChangeForSub import NewestStateChangeForSub as NewestStateChangeForSubMySQL

class NewestStateChangeForSub(NewestStateChangeForSubMySQL):
    pass
