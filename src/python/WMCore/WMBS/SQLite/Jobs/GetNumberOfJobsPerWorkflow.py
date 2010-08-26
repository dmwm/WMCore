#!/usr/bin/env python
"""
_GetNumberOfJobsPerWorkflow_

SQLite implementation of Jobs.GetNumberOfJobsPerWorkflow
"""

__all__ = []
__revision__ = "$Id: GetNumberOfJobsPerWorkflow.py,v 1.1 2010/04/26 20:35:56 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging

from WMCore.WMBS.MySQL.Jobs.GetNumberOfJobsPerWorkflow import \
     GetNumberOfJobsPerWorkflow as MySQLGetNumberOfJobsPerWorkflow


class GetNumberOfJobsPerWorkflow(MySQLGetNumberOfJobsPerWorkflow):
    """
    Same as MySQL

    """
