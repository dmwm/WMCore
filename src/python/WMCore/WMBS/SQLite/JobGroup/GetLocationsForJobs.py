#!/usr/bin/env python
"""
_GetLocationsForJobs_

SQLite implementation of JobGroup.GetLocationsForJobs
"""

__all__ = []
__revision__ = "$Id: GetLocationsForJobs.py,v 1.1 2009/09/15 16:07:01 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import inspect

from WMCore.WMBS.MySQL.JobGroup.GetLocationsForJobs import GetLocationsForJobs as MySQLGetLocationsForJobs

class GetLocationsForJobs(MySQLGetLocationsForJobs):
    """
    Identical to MySQL version
    """
