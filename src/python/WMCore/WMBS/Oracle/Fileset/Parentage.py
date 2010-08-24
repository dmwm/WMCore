#!/usr/bin/env python
"""
_Parentage_

Oracle implementation of Fileset.Parentage

"""
__all__ = []
__revision__ = "$Id: Parentage.py,v 1.3 2008/12/05 21:06:26 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Fileset.Parentage import Parentage as FilesetParentageMySQL

class Parentage(FilesetParentageMySQL):
    sql = FilesetParentageMySQL.sql