#!/usr/bin/env python
"""
_GetTask_

Oracle implementation of Jobs.Task
"""

__all__ = []
__revision__ = "$Id: GetTask.py,v 1.1 2009/10/23 19:24:07 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging

from WMCore.WMBS.MySQL.Jobs.GetTask import GetTask as MySQLGetTask

class GetTask(MySQLGetTask):
    """
    Identical to MySQL version
    """
