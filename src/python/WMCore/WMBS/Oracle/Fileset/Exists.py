#!/usr/bin/env python
"""
_Exists_

Oracle implementation of Fileset.Exists

"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.3 2008/12/05 21:06:26 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Fileset.Exists import Exists as ExistsFilesetMySQL

class Exists(ExistsFilesetMySQL):
    sql = ExistsFilesetMySQL.sql