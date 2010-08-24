#!/usr/bin/env python
"""
_HTTPFrontEnd_

Component that runs a CherryPy based web server to provide HTTP
access to the JobCreator cache.

May also add some interactive monitoring as time goes on.

Introduces a dependency on the cherrypy package

"""
import socket
import logging
import os
import cherrypy
from cherrypy.lib.static import serve_file
#from ProdAgentCore.Configuration import prodAgentName
#from ProdAgentCore.Configuration import loadProdAgentConfiguration
from WMCore.Agent.Configuration import loadConfigurationFile
from MessageService.MessageService import MessageService

import ProdAgentCore.LoggingUtils as LoggingUtils
# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
# we do not import failure handlers as they are dynamicly
# loaded from the config file.
from WMCore.WMFactory import WMFactory

from cherrypy.lib.static import serve_file
import logging

factory = WMFactory('generic')

class Downloader:
    """
    _Downloader_

    Serve files from the JobCreator Cache via HTTP

    """
    def __init__(self, rootDir):
        self.rootDir = rootDir

    def index(self, filepath):
        """
        _index_

        index response to download URL, serves the file
        requested

        """
        pathInCache = os.path.join(self.rootDir, filepath)
        logging.debug("Download Agent serving file: %s" % pathInCache)
        return serve_file(pathInCache, "application/x-download", "attachment")
    index.exposed = True

class Root:
    """
    _Root_

    Main index page for the component, will appear as the index page
    of the toplevel address

    """
    def __init__(self, myUrl):
        self.myUrl = myUrl
        self.components = []
 
    def addComponent(self, componentPath, config):
        # take the last field of the module name for the web page name
        oneWordName = componentPath.split('.')[-1]
        self.__dict__[oneWordName] = factory.loadObject(componentPath, config)
        self.components.append(oneWordName)

    def index(self):
        html = "<html><body><h2>HTTPFrontEnd </h2>\n "
        html += "<table>\n"
        html += "<tr><th>Service</th><th>Description</th></tr>\n"
        for component in self.components:
          html += "<tr><td><a href=\"%s/%s\">%s</a></td><td>%s</td></tr>\n" % (
            self.myUrl, component, component, self.__dict__[component].__doc__)
        html += """</table></body></html>"""
        return html

    index.exposed = True

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
        
        
        root = Root(baseUrl)
        for component in self.config.HTTPFrontEnd.components:
            root.addComponent(component, self.config)
        root.download = Downloader(self.config.Downloader.dir)
        cherrypy.tree.mount(root)
        cherrypy.server.quickstart()
        cherrypy.engine.start()


    def stop(self):
        cherrypy.engine.stop()
        

if __name__ == '__main__':
    config = loadConfigurationFile('DefaultConfig.py')
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
    config.CoreDatabase.user = 'root'
    config.CoreDatabase.passwd = 'XXXXXXX'
    config.CoreDatabase.hostname = 'localhost'
    config.CoreDatabase.name = 'wmbs'

    harness = HTTPFrontEnd(config)
    harness.prepareToStart()
    harness.handleMessage("HTTPFrontendStart", "GOGOGOGOGO")
    #harness.handleMessage("HTTPFrontendStop", "WHOAWHOAWHOA")
    harness.startComponent()
