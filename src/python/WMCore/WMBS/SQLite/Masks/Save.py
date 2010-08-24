#!/usr/bin/env python
"""
_Save_

SQLite implementation of Masks.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.1 2008/11/21 17:13:35 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Masks.Save import Save as SaveMySQL

class Save(SaveMySQL):
    def execute(self, jobid, mask):
        SaveMySQL.execute(self, jobid, mask)
