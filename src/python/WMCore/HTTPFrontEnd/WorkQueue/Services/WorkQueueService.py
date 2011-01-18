#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for WMBS Monitoring.
"""

from WMCore.Wrappers import JsonWrapper
from WMCore.HTTPFrontEnd.WorkQueue.Services.ServiceInterface import ServiceInterface
from cherrypy import HTTPError

from WMCore.WorkQueue.WorkQueue import globalQueue, localQueue
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueError

from functools import partial
from os import path

# change to a url here so unit tests aren't affected
def wrapGetWork(workqueue, *args, **kwargs):
    """Change url's to be web-accessible"""

    result = workqueue.getWork(*args, **kwargs)
    for item in result:
        item['url'] = "%s/wf/%s" % (workqueue.params['QueueURL'],
                                    path.basename(item['url']))
        if item.has_key('mask_url'):
            item['mask_url'] = "%s/wf/%s" % (workqueue.params['QueueURL'],
                                             path.basename(item['mask_url']))
    return result

def serveWorkflow(workqueue, name):
    """Return a workflow from the cache"""
    name = path.normpath(path.join(workqueue.params['CacheDir'], name))
    if path.commonprefix([name, workqueue.params['CacheDir']]) != workqueue.params['CacheDir']:
        raise HTTPError(403)
    if not path.exists(name):
        raise HTTPError(404, "%s not found" % name)
    data = ''
    with open(name, 'rb') as infile:
        data = infile.read()
    return data


class WorkQueueService(ServiceInterface):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def register(self):

        if not hasattr(self.model.config, 'queueParams'):
            self.model.config.queueParams = {}


        # we don't populate wmbs - WorkQueueManager does (in the LocalQueue)
        # in getWork (getWork REST call shouldn't be called
        # to the localQueue Service since we can't force that added the trick here)
        self.model.config.queueParams['PopulateFilesets'] = False

        if self.model.config.level == "GlobalQueue":
            self.wq = globalQueue(logger=self.model, dbi=self.model.dbi,
                                  **self.model.config.queueParams)
        elif self.model.config.level == "LocalQueue":
            self.wq = localQueue(logger=self.model, dbi=self.model.dbi,
                                  **self.model.config.queueParams)
        else:
            raise WorkQueueError("%s workqueue level is unknown" %
                                 self.model.config.level)


        self.model._addMethod('POST', 'getwork', partial(wrapGetWork, self.wq),
                             args=["siteJobs", "pullingQueueUrl", "team"])
        
        self.model._addMethod('GET', 'status', self.wq.status, 
                             args = ["status", "before", "after", "elementIDs", "dictKey"],
                             validation = [self.statusValidation])
        self.model._addMethod('GET', 'wf', partial(serveWorkflow, self.wq), args = ['name'])

        # All the service provided below need secure layer(OpenID for authentication)
        # TODO: if it allows pass wmspec pickled file directly instead of url location.
        # change the verb to post
        self.model._addMethod('PUT', 'queuework', self.wq.queueWork,
                             args = ["wmspecUrl", "team", "request"])

        self.model._addMethod('PUT', 'synchronize', self.wq.synchronize,
                             args = ["child_url", "child_report"])
        
        self.model._addMethod('PUT', 'failwork', self.wq.failWork, args = ["elementIDs"])
        self.model._addMethod('PUT', 'donework', self.wq.doneWork, args = ["elementIDs"])
        self.model._addMethod('PUT', 'cancelwork', self.wq.cancelWork,
                             args = ["elementIDs", "id_type"])
    
    #TODO if it needs to be validated, add validation
    #The only requirement of validation function is take input (dict) type return input.
            
    def statusValidation(self, input):
        """
        validate status function and do the type conversion if the argument 
        requires non string 
        """
        if input.has_key("before") and input["before"]:
            input["before"] = int(input["before"])
        if input.has_key("after") and input["after"]:
            input["after"] = int(input["after"])
        
        if input.has_key("elementIDs"):    
            if type(input["elementIDs"]) != list:
                input["elementIDs"] = [int(input["elementIDs"])]
            else:
                input["elementIDs"] = map(int, input["elementIDs"])
        return input
