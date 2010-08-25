#!/usr/bin/env python
"""
_GetSubTypes_

Oracle implementation of Jobs.GetSubTypes
"""

__all__ = []
__revision__ = "$Id: GetSubTypes.py,v 1.1 2010/01/22 20:47:00 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging

from WMCore.WMBS.MySQL.Subscriptions.GetSubTypes import GetSubTypes as MySQLGetSubTypes



class GetSubTypes(MySQLGetSubTypes):
    """
    Identical to MySQL version

    """
