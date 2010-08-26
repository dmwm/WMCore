#!/usr/bin/env python
"""
_Load_

Oracle implementation of Masks.Load
"""

__all__ = []
__revision__ = "$Id: Load.py,v 1.2 2008/12/05 21:06:24 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Masks.Load import Load as LoadMasksMySQL

class Load(LoadMasksMySQL):
    sql = LoadMasksMySQL.sql