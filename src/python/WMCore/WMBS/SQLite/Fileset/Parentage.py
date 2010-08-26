#!/usr/bin/env python
"""
_Parentage_

SQLite implementation of Fileset.Parentage

"""
__all__ = []
__revision__ = "$Id: Parentage.py,v 1.2 2008/11/20 21:54:26 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Fileset.Parentage import Parentage as FilesetParentageMySQL

class Parentage(FilesetParentageMySQL):
    sql = FilesetParentageMySQL.sql