#!/usr/bin/env python
"""
_Save_

Oracle implementation of Masks.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.2 2008/12/05 21:06:24 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Masks.Save import Save as SaveMasksMySQL

class Save(SaveMasksMySQL):
    def execute(self, jobid, mask):
        SaveMasksMySQL.execute(self, jobid, mask)
