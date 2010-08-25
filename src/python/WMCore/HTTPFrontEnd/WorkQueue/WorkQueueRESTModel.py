#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for Workqueue DAS.
"""

from WMCore.Wrappers import JsonWrapper
from WMCore.WebTools.RESTModel import RESTModel
from WMCore.WMFactory import WMFactory

class WorkQueueRESTModel(RESTModel):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def __init__(self, config = {}):
        RESTModel.__init__(self, config)
        
        if hasattr(config, "serviceModules"):
            #TODO: move this out of __init__ possible
            factory = WMFactory('WorkqueueServices')
            
            for module in config.serviceModules:
                service = factory.loadObject(module, self)
                service.register()