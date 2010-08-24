#!/usr/bin/env python
"""
_HTTPFrontEnd_

Component that runs a CherryPy based web server to provide HTTP
access to the JobCreator cache.

May also add some interactive monitoring as time goes on.

Introduces a dependency on the cherrypy package

"""
import os
import cherrypy
from WMCore.Configuration import loadConfigurationFile
from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory
from WMCore.HTTPFrontEnd import Downloader
from WMCore.WebTools.Root import Root

factory = WMFactory('generic')


class HTTPFrontEnd(Harness):
    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        #self.start()


    def preInitialization(self):
        """
        Initializes plugins for different messages
        """
        self.messages['HTTPFrontendStart'] = \
            factory.loadObject('WMComponent.HTTPFrontend.HTTPFrontendStartHandler', self)
        self.messages['HTTPFrontendStop'] = \
            factory.loadObject('WMComponent.HTTPFrontend.HTTPFrontendStopHandler', self)
 

    def start(self):
        """
        _startComponent_

        Start up the cherrypy service for this component

        """
        root = Root(self.config)
        root.configureCherryPy()
        root.loadPages()
        root.makeIndex()
        cherrypy.engine.start()
        cherrypy.engine.block()


    def stop(self):
        cherrypy.engine.stop()
        

