#!/usr/bin/env python
"""
_GetTask_

Oracle implementation of Jobs.Task
"""

__all__ = []



import logging

from WMCore.WMBS.MySQL.Jobs.GetTask import GetTask as MySQLGetTask

class GetTask(MySQLGetTask):
    """
    Identical to MySQL version
    """
