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
from WMComponent.HTTPFrontend import Downloader
from WMCore.WebTools.Root import Root

factory = WMFactory('generic')


class HTTPFrontEnd(Harness):
    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)

        self.staticDir = os.path.join(self.config.HTTPFrontEnd.ComponentDir, "static")
        if not os.path.exists(self.staticDir):
            os.makedirs(self.staticDir)
        
        if self.config.HTTPFrontEnd.Logfile == None:
            self.config.HTTPFrontEnd.Logfile = os.path.join(self.config.HTTPFrontEnd.ComponentDir,
                                                "ComponentLog")
        if self.config.HTTPFrontEnd.HTTPLogfile == None:
            self.config.HTTPFrontEnd.HTTPLogfile = os.path.join(self.config.HTTPFrontEnd.ComponentDir,
                                                    "HTTPLog")
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
        cherrypy.config.update({'environment': 'production',
                                'log.error_file': self.config.HTTPFrontEnd.HTTPLogfile,
                                'log.screen': True})
        cherrypy.config.update({
        "global" : {
        "server.socket_host" :  self.config.HTTPFrontEnd.Host,
        "server.socket_port" :  self.config.HTTPFrontEnd.Port,
        "server.thread_pool" :  self.config.HTTPFrontEnd.ThreadPool,
        }})
        
        baseUrl = "http://%s:%s" % (
            self.config.HTTPFrontEnd.Host, self.config.HTTPFrontEnd.Port)
        
        
        root = Root(self.config)
        root.loadPages()
        cherrypy.tree.mount(root)
        cherrypy.server.quickstart()
        cherrypy.engine.start()


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
    harness.startComponent()
