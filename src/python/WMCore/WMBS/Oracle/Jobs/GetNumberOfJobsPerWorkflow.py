#!/usr/bin/env python
"""
_GetNumberOfJobsPerWorkflow_

Oracle implementation of Jobs.GetNumberOfJobsPerWorkflow
"""

__all__ = []



import logging

from WMCore.WMBS.MySQL.Jobs.GetNumberOfJobsPerWorkflow import \
     GetNumberOfJobsPerWorkflow as MySQLGetNumberOfJobsPerWorkflow


class GetNumberOfJobsPerWorkflow(MySQLGetNumberOfJobsPerWorkflow):
    """
    Same as MySQL

    """
