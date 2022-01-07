#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Oracle implementation for retrieving the total number of jobs
for a given workflow in a given wmbs status
"""

from __future__ import print_function, division

from WMCore.WMBS.MySQL.Jobs.GetNumberOfJobsForWorkflowStatus import GetNumberOfJobsForWorkflowStatus \
    as MySQLGetNumberOfJobsForWorkflowStatus


class GetNumberOfJobsForWorkflowStatus(MySQLGetNumberOfJobsForWorkflowStatus):
    """
    Exactly the same implementation as of the MySQL one
    """
    pass
