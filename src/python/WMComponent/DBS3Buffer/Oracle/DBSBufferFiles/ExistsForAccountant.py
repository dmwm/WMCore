#!/usr/bin/env python
"""
_ExistsForAccountant_

Oracle implementation of Files.ExistsForAccountant
"""

__all__ = []



from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.ExistsForAccountant import ExistsForAccountant as MySQLExistsForAccountant

class ExistsForAccountant(MySQLExistsForAccountant):
    """
    This is highly specialized.  You shouldn't confuse it with
    a normal Exists DAO
    """
