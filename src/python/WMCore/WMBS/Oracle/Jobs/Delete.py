#!/usr/bin/env python
"""
_Delete_

Oracle implementation of Jobs.Delete

"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/11/24 21:51:39 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.Delete import Delete as DeleteJobMySQL

class Delete(DeleteJobMySQL):
    sql = DeleteJobMySQL.sql