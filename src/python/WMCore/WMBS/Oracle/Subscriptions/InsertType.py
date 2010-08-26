#!/usr/bin/env python
"""
_InsertType_

Oracle implementation of Subscription.InsertType
"""

__revision__ = "$Id: InsertType.py,v 1.1 2010/02/09 17:51:31 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.InsertType import InsertType as InsertTypeMySQL

class InsertType(InsertTypeMySQL):
    sql = """INSERT INTO wmbs_sub_types (id, name)
               SELECT wmbs_sub_types_SEQ.nextval, :name AS name FROM DUAL WHERE NOT EXISTS
                (SELECT name FROM wmbs_sub_types WHERE name = :name)"""
