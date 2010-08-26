#!/usr/bin/env python
"""
_DeleteFileset_

Oracle implementation of Fileset.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.3 2008/12/05 21:06:26 sryu Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.WMBS.MySQL.Fileset.Delete import Delete as DeleteFilesetMySQL

class Delete(DeleteFilesetMySQL):
    sql = DeleteFilesetMySQL.sql