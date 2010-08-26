#!/usr/bin/env python

"""
Oracle implementation of AddCKType
"""


__revision__ = "$Id: AddCKType.py,v 1.1 2009/12/02 19:35:07 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.AddCKType import AddCKType as MySQLAddCKType

class AddCKType(MySQLAddCKType):
    sql = """INSERT INTO wmbs_checksum_type (id, type)
               VALUES (wmbs_checksum_type_SEQ.nextval, :cktype)"""
                

