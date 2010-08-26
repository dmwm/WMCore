#!/usr/bin/env python
"""
_Delete_

Oracle implementation of Masks.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2008/12/05 21:06:24 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Masks.Delete import Delete as DeleteMasksMySQL

class Delete(DeleteMasksMySQL):
    sql = DeleteMasksMySQL.sql
    