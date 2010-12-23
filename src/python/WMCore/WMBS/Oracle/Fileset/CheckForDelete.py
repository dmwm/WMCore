#!/usr/bin/env python
"""
_CheckForDelete_

Oracle implementation of DeleteCheck

"""
__all__ = []



from WMCore.WMBS.MySQL.Fileset.CheckForDelete import CheckForDelete as MySQLCheckForDelete

class CheckForDelete(MySQLCheckForDelete):
    """
    Oracle implementation

    """
