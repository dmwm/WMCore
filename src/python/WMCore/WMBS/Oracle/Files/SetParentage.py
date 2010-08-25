#!/usr/bin/env python
"""
Oracle implementation of File.SetParentage

Make the parentage link between two file lfns in bulk
"""
__all__ = []
__revision__ = "$Id: SetParentage.py,v 1.1 2010/08/13 16:41:57 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.SetParentage import SetParentage as MySQLSetParentage

class SetParentage(MySQLSetParentage):
    """
    _SetParentage_


    Identical to MySQL version
    """
