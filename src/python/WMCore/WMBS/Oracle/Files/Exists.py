#!/usr/bin/env python
"""
_Exists_

Oracle implementation of Files.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2008/10/22 19:08:28 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Oracle.Base import OracleBase
from WMCore.WMBS.MySQL.Files.Exists import Exists as FilesExistsMySQL

class Exists(FilesExistsMySQL, OracleBase):
    sql = FilesExistsMySQL.sql
