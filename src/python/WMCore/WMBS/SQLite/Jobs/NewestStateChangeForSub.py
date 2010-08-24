#!/usr/bin/env python
"""
_NewestStateChangeForSub_

SQLite implementation of Jobs.NewestStateChangeForSub
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.NewestStateChangeForSub import NewestStateChangeForSub as NewestStateChangeForSubMySQL

class NewestStateChangeForSub(NewestStateChangeForSubMySQL):
    pass
