#!/usr/bin/env python
"""
_Exists_

Oracle implementation of Jobs.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2008/11/24 21:51:39 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.Exists import Exists as ExistsJobMySQL

class Exists(ExistsJobMySQL):
    sql = ExistsJobMySQL.sql