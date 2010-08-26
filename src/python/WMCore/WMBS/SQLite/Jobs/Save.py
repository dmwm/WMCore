#!/usr/bin/env python
"""
_Save_

SQLite implementation of Jobs.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.2 2009/05/11 14:47:48 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.Save import Save as SaveMySQL

class Save(SaveMySQL):
    pass
