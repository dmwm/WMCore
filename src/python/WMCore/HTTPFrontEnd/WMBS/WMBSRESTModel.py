#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for WMBS Monitoring.
"""

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.MySQL.Jobs.Monitoring import JobsByState

class WMBSRESTModel(RESTModel):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def __init__(self, config = {}):
        RESTModel.__init__(self, config)
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self,
                                     dbinterface = self.dbi)

    def handle_get(self, args=[], kwargs={}):
        if args[0].lower() == 'jobs':
            jbs = JobsByState(self, self.dbi)
            jobcounts = jbs.execute()
            return {'message': jobcounts}