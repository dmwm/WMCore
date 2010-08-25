#!/usr/bin/env python
"""
_HTTPFrontEnd_

Component that runs a CherryPy based web server to provide HTTP
access to the JobCreator cache.

May also add some interactive monitoring as time goes on.

Introduces a dependency on the cherrypy package

"""
import socket
import os
import cherrypy
from WMCore.Configuration import loadConfigurationFile
from MessageService.MessageService import MessageService

import ProdAgentCore.LoggingUtils as LoggingUtils
# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
# we do not import failure handlers as they are dynamicly
# loaded from the config file.
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
        

if __name__ == '__main__':
    config = loadConfigurationFile('DefaultConfig.py')
    config.section_("Agent")
    config.Agent.contact = "rickw@caltech.edu"
    config.Agent.teamName = "Dodgers"
    config.Agent.agentName = "Manny Ramirez"

    config.section_("General")
    config.General.workDir = os.getenv("PRODAGENT_WORKDIR")
    config.section_("CoreDatabase")
    config.CoreDatabase.dialect = 'mysql'
    #config.CoreDatabase.socket = os.getenv("DBSOCK")
    #config.CoreDatabase.user = os.getenv("DBUSER")
    #config.CoreDatabase.passwd = os.getenv("DBPASS")
    #config.CoreDatabase.hostname = os.getenv("DBHOST")
    #config.CoreDatabase.name = os.getenv("DBNAME")
    config.CoreDatabase.socket = '/home/rpw/work/mysqldata/mysql.sock'
    config.CoreDatabase.user = 'some_user'
    config.CoreDatabase.passwd = 'some_pass'
    config.CoreDatabase.hostname = 'localhost'
    config.CoreDatabase.name = 'wmbs'

    harness = HTTPFrontEnd(config)
    harness.prepareToStart()
    harness.handleMessage("HTTPFrontendStart", "GOGOGOGOGO")
    #harness.handleMessage("HTTPFrontendStop", "WHOAWHOAWHOA")
    #harness.startComponent()
