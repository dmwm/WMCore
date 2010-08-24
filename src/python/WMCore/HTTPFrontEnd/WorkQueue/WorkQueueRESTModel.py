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
    A REST Model for WorkQueue. this is only has loading module functions and
    processing parameter functions.
    
    Actual APIs are organized in the Service Module.
    TODO: convince Simon to move this in RESTModel.
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
        depending on the content type.
        
        TODO: corrently it only works with cjson not json from python2.6.
        There is issues of converting unit code to string.
        """
        handler = ContentTypeHandler()
        return handler.convertToParam(args, kwargs)