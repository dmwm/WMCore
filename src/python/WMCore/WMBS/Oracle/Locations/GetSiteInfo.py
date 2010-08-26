#!/usr/bin/env python
"""
_GetSiteInfo_

Oracle implementation of Locations.GetSiteInfo
"""

__all__ = []
__revision__ = "$Id: GetSiteInfo.py,v 1.1 2010/02/15 17:33:23 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Locations.GetSiteInfo import GetSiteInfo as MySQLGetSiteInfo

import logging

class GetSiteInfo(MySQLGetSiteInfo):
    """
    Same as MySQL version

    """
