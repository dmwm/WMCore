#!/usr/bin/env python

import unittest
import logging
import cherrypy

from WMCore.Configuration import Configuration
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.WebTools.Root import Root

class WorkQueueServiceTest(unittest.TestCase):
    """
    Test WorkQueue Service client
    It will start WorkQueue RESTService
    Server DB is SQlite.
    Client DB sets from environment variable. 
    """
    def configureWorkQueueServer(self):
        config = Configuration()

        config.component_('Webtools')
        config.Webtools.application = 'WorkQueueService'
        config.Webtools.log_screen = False
        config.Webtools.access_file = '/dev/null'
        config.Webtools.error_file = '/dev/null'
        config.Webtools.port = 6660
        config.Webtools.host = "cmssrv18.fnal.gov"
        config.component_('WorkQueueService')
        
        config.WorkQueueService.templates = '/tmp'
        config.WorkQueueService.admin = 'your@email.com'
        config.WorkQueueService.title = 'CMS WMCore/HTTPFrontEnd/WorkQueue Unit Tests'
        config.WorkQueueService.description = 'Dummy server for the running of unit tests' 
        
        config.WorkQueueService.section_('views')
        # These are all the active pages that Root.py should instantiate
        active = config.WorkQueueService.views.section_('active')
        workqueue = active.section_('workqueue')
        # The class to load for this view/page
        workqueue.object = 'WMCore.WebTools.RESTApi'
        workqueue.templates = '/tmp'
        workqueue.database = 'sqlite://'
        workqueue.section_('model')
        workqueue.model.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel'
        workqueue.section_('formatter')
        workqueue.formatter.object = 'WMCore.WebTools.DASRESTFormatter'
        workqueue.formatter.templates = '/tmp'
        
        rt = Root(config)
        return rt
    
    def startWQServer(self):
        server = self.configureWorkQueueServer()
        server.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        return server
    
    def setUp(self):
        """
        setUP global values
        """
        wmbsTestDS = "http://cmssrv18.fnal.gov:6660/workqueue/"
        
        self.params = {}
        self.params['endpoint'] = wmbsTestDS
        
    def testStartServer(self):
        server = self.startWQServer()    
        server.stop()
        
    def testGetWork(self):
        
        server = self.startWQServer()
        wqApi = WorkQueue(self.params)

        print wqApi.getWork({'SiteB' : 15, 'SiteA' : 15}, "http://test.url")
        server.stop()
        
    def testSynchronize(self):
        server = self.startWQServer()
        
        wqApi = WorkQueue(self.params)
        childUrl = "http://test.url"
        childResources = [{'ParentQueueId' : 1, 'Status' : 'Available'}]
        print wqApi.synchronize(childUrl, childResources)
        
        childUrl = "http://test.url"
        childResources = []
        print wqApi.synchronize(childUrl, childResources)
        server.stop()
        
    def testStatusChange(self):
        
        server = self.startWQServer()
        
        wqApi = WorkQueue(self.params)

        print wqApi.gotWork([1])
        print wqApi.status()
        print wqApi.doneWork([1])
        print wqApi.status()
        print wqApi.failWork([1])
        print wqApi.status()
        print wqApi.cancelWork([1])
        print wqApi.status()
        server.stop()
        
if __name__ == '__main__':

    unittest.main()