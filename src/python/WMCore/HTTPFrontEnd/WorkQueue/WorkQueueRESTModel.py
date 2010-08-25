#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for Workqueue DAS.
"""

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.WMFactory import WMFactory
from WMCore.HTTPFrontEnd.WorkQueue.ContentTypeHandler import ContentTypeHandler

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
                
    def processParams(self, args, kwargs):
        """
        overwrite base class processParams to handle encoding and decoding
        depending on the content type
        """
        handler = ContentTypeHandler()
        return handler.convertToParam(args, kwargs)