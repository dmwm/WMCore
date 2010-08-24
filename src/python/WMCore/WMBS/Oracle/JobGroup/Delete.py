#!/usr/bin/env python
"""
_Delete_

Oracle implementation of JobGroup.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2008/12/05 21:06:26 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.JobGroup.Delete import Delete as DeleteJobGroupMySQL

class Delete(DeleteJobGroupMySQL):
    sql = DeleteJobGroupMySQL.sql