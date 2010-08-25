#!/usr/bin/env python
"""
_New_

SQLite implementation of Jobs.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.5 2009/05/11 14:47:48 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Jobs.New import New as NewMySQL

class New(NewMySQL):
    pass
