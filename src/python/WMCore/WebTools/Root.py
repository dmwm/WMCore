#!/usr/bin/env python
"""
_Root_

The root object for a webtools application. It loads all the different views and
starts an appropriately configured CherryPy instance. Views are loaded
dynamically and can be turned on/off via configuration file.

"""
from __future__ import print_function

from builtins import str, bytes
from future.utils import viewitems, listitems

import logging
import os
import socket
import sys
import time
from argparse import ArgumentParser
from pprint import pformat

# CherryPy
import cherrypy
from cherrypy._cplogging import LogManager

# Logging
import WMCore.WMLogging
from Utils.Utilities import lowerCmsHeaders
# configuration and arguments
from WMCore.Agent.Daemon.Create import createDaemon
from WMCore.Agent.Daemon.Details import Details
from WMCore.Agent.Harness import Harness
from WMCore.Configuration import Configuration, ConfigSection
from WMCore.Configuration import loadConfigurationFile
# Factory to load pages dynamically
from WMCore.WMFactory import WMFactory
from WMCore.WebTools.FrontEndAuth import FrontEndAuth, NullAuth
from WMCore.WebTools.Welcome import Welcome

lastTest = ""


def mytime():
    """
    Utility to time stamp start of request handling.
    """
    cherrypy.request.start_time = time.time()


def myproxy(base=None):
    """
    Utility aid in request handling from behind a proxy.
    """
    request = cherrypy.request
    scheme = request.headers.get('X-Forwarded-Proto', request.base[:request.base.find("://")])
    base = request.headers.get('X-Forwarded-Host', base)
    if not base:
        port = cherrypy.request.local.port
        if port == 80:
            base = 'localhost'
        else:
            base = 'localhost:%s' % port

    base = base.split(',')[0].strip()
    if base.find("://") == -1:
        base = scheme + "://" + base
    request.base = base

    xff = request.headers.get('X-Forwarded-For')
    if xff:
        xff = xff.split(',')[0].strip()
        request.remote.ip = xff


class WTLogger(LogManager):
    """
    A logger that logs how we want it to.
    """

    def __init__(self, *args, **kwargs):
        self.host = socket.gethostname()
        LogManager.__init__(self, *args, **kwargs)

    def access(self):
        """
        Record a client access
        """
        request = cherrypy.request
        remote = request.remote
        response = cherrypy.response
        inheaders = lowerCmsHeaders(request.headers)
        outheaders = response.headers
        # identify size of body from HTTP Content-Length header
        rbytes = int(cherrypy.request.headers.get('Content-Length', 0))
        if not rbytes:
            try:
                # request.rfile.rfile.bytes_read is a custom CMS web
                #  cherrypy patch not always available, hence the test
                rbytes = (getattr(request.rfile, 'rfile', None)
                        and getattr(request.rfile.rfile, "bytes_read", None)
                        and request.rfile.rfile.bytes_read) or "-"
            except:
                try:
                    # this will work only when body is read from request
                    rbytes = cherrypy.request.body.fp.bytes_read
                except:
                    rbytes = "-"        
        msg = ('%(t)s %(H)s %(h)s "%(r)s" %(s)s'
               + ' [data: %(i)s in %(b)s out %(T).0f us ]'
               + ' [auth: %(AS)s "%(AU)s" "%(AC)s" ]'
               + ' [ref: "%(f)s" "%(a)s" ]') % {'t': self.time(),
                                                'H': self.host,
                                                'h': remote.name or remote.ip,
                                                'r': request.request_line,
                                                's': response.status,
                                                'i': rbytes,
                                                'b': outheaders.get('Content-Length', '') or "-",
                                                'T': (time.time() - request.start_time) * 1e6,
                                                'AS': inheaders.get("cms-auth-status", "-"),
                                                'AU': inheaders.get("cms-auth-cert",
                                                                    inheaders.get("cms-auth-host", "")),
                                                'AC': getattr(request.cookie.get("cms-auth", None), "value", ""),
                                                'f': inheaders.get("Referer", ""),
                                                'a': inheaders.get("User-Agent", "")}
        self.access_log.log(logging.INFO, msg)


class Root(Harness):
    """
    Create the appropriate cherrypy root object
    """

    def __init__(self, config, webApp=None, testName=""):
        """
        Initialise the object, pull out the necessary pieces of the configuration
        """
        self.homepage = None
        self.mode = 'component'
        self.testName = testName
        if webApp is None:
            Harness.__init__(self, config, compName="Webtools")
            self.appconfig = config.section_(self.config.Webtools.application)
            self.app = self.config.Webtools.application
            self.secconfig = config.component_("SecurityModule")
            self.serverConfig = config.section_("Webtools")
            self.coreDatabase = config.section_("CoreDatabase")
            self.mode = 'standalone'
        else:
            Harness.__init__(self, config, compName=webApp)
            self.appconfig = config.section_(webApp)
            WMCore.WMLogging.setupRotatingHandler(os.path.join(self.appconfig.componentDir, "%s.log" % webApp))
            self.app = webApp
            self.secconfig = getattr(self.appconfig, "security")
            self.serverConfig = config.section_(webApp).section_("Webtools")
            self.coreDatabase = config.section_("CoreDatabase")

        return

    def getLastTest(self):
        global lastTest
        return lastTest

    def setLastTest(self):
        global lastTest
        lastTest = self.testName

    def _validateConfig(self):
        """
        Check that the configuration has the required sections
        """
        config_dict = self.appconfig.dictionary_()
        must_have_keys = ['admin', 'description', 'title']
        for key in must_have_keys:
            msg = "Application configuration '%s' does not contain '%s' key" \
                  % (self.app, key)
            assert key in config_dict, msg

    def _configureCherryPy(self):
        """
        _configureCherryPy_
        Configure the CherryPy server, ignoring items in the configuration file that
        aren't CherryPy configurables.
        """
        configDict = self.serverConfig.dictionary_()

        # If you add configurables update the testLongHandConfigurables in Root_t
        configurables = ['engine', 'hooks', 'log', 'request', 'response',
                         'server', 'tools', 'wsgi', 'checker']
        # Deal with "long hand" configuration variables
        for i in configurables:
            if i in configDict:
                for config_param, param_value in viewitems(configDict[i].dictionary_()):
                    if isinstance(param_value, ConfigSection):
                        # TODO: make this loads better
                        for child_param, child_param_value in viewitems(param_value.dictionary_()):
                            cherrypy.config["%s.%s.%s" % (i, config_param, child_param)] = child_param_value
                    elif isinstance(param_value, (str, bytes, int)):
                        cherrypy.config["%s.%s" % (i, config_param)] = param_value
                    else:
                        raise Exception("Unsupported configuration type: %s" % type(param_value))

        # which we then over write with short hand variables if necessary
        cherrypy.config["server.environment"] = configDict.get("environment", "production")

        # Set up the tools and the logging
        cherrypy.tools.time = cherrypy.Tool('on_start_resource', mytime)
        cherrypy.config.update({'tools.time.on': True})

        if configDict.get("proxy_base", False):
            cherrypy.tools.proxy = cherrypy.Tool('before_request_body', myproxy, priority=30)
            cherrypy.config.update({
                'tools.proxy.on': True,
                'tools.proxy.base': configDict["proxy_base"]
            })

        cherrypy.log = WTLogger()
        cherrypy.config["log.screen"] = bool(configDict.get("log_screen", False))

        cherrypy.config.update({
            'tools.expires.on': True,
            'tools.expires.secs': configDict.get("expires", 300),
            'tools.response_headers.on': True,
            'tools.etags.on': True,
            'tools.etags.autotags': True,
            'tools.encode.on': True,
            'tools.encode.text_only': False,
            'tools.gzip.on': True,
        })

        if cherrypy.config["server.environment"] == "production":
            # If we're production these should be set regardless
            cherrypy.config["request.show_tracebacks"] = False
            cherrypy.config["engine.autoreload.on"] = False
            # In production mode only allow errors at WARNING or greater to the log
            err_lvl = max((configDict.get("error_log_level", logging.WARNING), logging.WARNING))
            acc_lvl = max((configDict.get("access_log_level", logging.INFO), logging.INFO))
            cherrypy.log.error_log.setLevel(err_lvl)
            cherrypy.log.access_log.setLevel(acc_lvl)
        else:
            print()
            print('THIS BETTER NOT BE A PRODUCTION SERVER')
            print()
            cherrypy.config["request.show_tracebacks"] = configDict.get("show_tracebacks", False)
            cherrypy.config["engine.autoreload.on"] = configDict.get("autoreload", False)
            # Allow debug output
            cherrypy.log.error_log.setLevel(configDict.get("error_log_level", logging.DEBUG))
            cherrypy.log.access_log.setLevel(configDict.get("access_log_level", logging.DEBUG))

        default_port = 8080
        if "server.socket_port" in cherrypy.config:
            default_port = cherrypy.config["server.socket_port"]
        cherrypy.config["server.thread_pool"] = configDict.get("thread_pool", 10)
        cherrypy.config["server.accepted_queue_size"] = configDict.get("accepted_queue_size", -1)
        cherrypy.config["server.accepted_queue_timeout"] = configDict.get("accepted_queue_timeout", 10)
        cherrypy.config["server.socket_port"] = configDict.get("port", default_port)
        cherrypy.config["server.socket_host"] = configDict.get("host", "0.0.0.0")
        # A little hacky way to pass the expire second to config
        self.appconfig.default_expires = cherrypy.config["tools.expires.secs"]

        # SecurityModule config
        # Registers secmodv2 into cherrypy.tools so it can be used through
        # decorators
        if self.secconfig.dictionary_().get('dangerously_insecure', False):
            cherrypy.tools.secmodv2 = NullAuth(self.secconfig)
        else:
            cherrypy.tools.secmodv2 = FrontEndAuth(self.secconfig)
            if hasattr(self.secconfig, "default"):
                # If the 'default' section is present, it will force the
                # authn/z to be called even for non-decorated methods
                cherrypy.config.update({'tools.secmodv2.on': True,
                                        'tools.secmodv2.role': self.secconfig.default.role,
                                        'tools.secmodv2.group': self.secconfig.default.group,
                                        'tools.secmodv2.site': self.secconfig.default.site})
        cherrypy.config.update({'tools.cpstats.on': configDict.get('cpstats', False)})
        cherrypy.config.update({'server.statistics': configDict.get('cpstats', False)})
        cherrypy.log.error_log.debug('Application %s initialised in %s mode', self.app, self.mode)
        cherrypy.log.access_log.info("Final CherryPy configuration: %s" % pformat(cherrypy.config))

    def _loadPages(self):
        """
        Load up all the pages in the configuration
        """
        factory = WMFactory('webtools_factory')

        globalconf = self.appconfig.dictionary_()
        del globalconf['views']
        the_index = ''
        if 'index' in globalconf:
            the_index = globalconf['index']
            del globalconf['index']

        for view in self.appconfig.views.active:
            # Iterate through each view's configuration and instantiate the class
            if view._internal_name != the_index:
                if 'instances' in globalconf:
                    for instance in globalconf['instances']:
                        self._mountPage(view, globalconf, factory, instance)
                else:
                    self._mountPage(view, globalconf, factory)

        if hasattr(self.appconfig.views, 'maintenance'):
            # for i in self.appconfig.views.maintenance:
            # TODO: Show a maintenance page with a 503 Service Unavailable header
            pass

    def _generate_config(self, view, globalconf, instance=None, is_index=False):
        """
        _generate_config_
        Create the configuration for a page by combining it's configuration
        with the global one
        """
        config = Configuration()
        view_config = config.component_(view._internal_name)
        view_config.application = self.app

        view_dict = view.dictionary_()
        for k in globalconf:
            # Add the global config to the view
            view_config.__setattr__(k, globalconf[k])
        for k in view_dict:
            # overwrite global if the local config is different
            view_config.__setattr__(k, view_dict[k])

        # TODO: remove bits we don't need

        if instance:
            # record the instance into the view's configuration
            view_config.instance = instance
            if hasattr(view, 'database') and hasattr(view.database, 'instances'):
                db_cfg = view.database.section_('instances')
                view_config.section_('database')
                view_config.database = db_cfg.section_(instance)

            if hasattr(view, 'security') and hasattr(view.security, 'instances'):
                security_cfg = view.security.section_('instances')
                view_config.section_('security')
                view_config.security = security_cfg.section_(instance)

        if 'database' in view_config.dictionary_():
            if not isinstance(view_config.database, (str, bytes)):
                if len(view_config.database.listSections_()) == 0:
                    if len(self.coreDatabase.listSections_()) > 0:
                        view_config.database.connectUrl = self.coreDatabase.connectUrl
                        if hasattr(self.coreDatabase, "socket"):
                            view_config.database.socket = self.coreDatabase.socket
        return view_config

    def _mountPage(self, view, globalconf, factory, instance=None, is_index=False):
        """
        _mountPage_
        Add the page to the CherryPy tree.
        """
        if is_index:
            mount_point = os.path.join('/', self.app.lower())
        elif instance:
            mount_point = os.path.join('/', self.app.lower(), instance, view._internal_name)
        else:
            mount_point = os.path.join('/', self.app.lower(), view._internal_name)
        view_config = self._generate_config(view, globalconf, instance, is_index)
        # component now contains the full configuration (global + view)
        # use this throughout

        cherrypy.log.error_log.debug("Loading %s", view_config._internal_name)
        # Load the object
        obj = factory.loadObject(view_config.object, view_config, getFromCache=False)
        # Attach the object to cherrypy's tree, at the name of the component
        cherrypy.tree.mount(obj, mount_point)
        msg = "%s available on %s/%s" % (view_config._internal_name,
                                         cherrypy.server.base(),
                                         mount_point)
        cherrypy.log.error_log.info(msg)

    def _makeIndex(self):
        """
        Create an index page, either from the configured page or a generic default
        welcome page.
        """
        globalconf = self.appconfig.dictionary_()
        if hasattr(self.appconfig, 'index'):
            factory = WMFactory('webtools_factory')
            view = getattr(self.appconfig.views.active, globalconf['index'])
            del globalconf['views']
            del globalconf['index']
            self._mountPage(view, globalconf, factory, is_index=True)

        else:
            cherrypy.log.error_log.info("No index defined for %s - instantiating default Welcome page", self.app)
            namesAndDocstrings = []
            # make a default Welcome
            for view in self.appconfig.views.active:
                if not getattr(view, "hidden", False):
                    viewName = view._internal_name
                    if 'instances' in globalconf:
                        for instance in globalconf['instances']:
                            mount_point = '/%s/%s/%s' % (self.app.lower(), instance, viewName)
                            viewObj = cherrypy.tree.apps[mount_point].root
                            docstring = viewObj.__doc__
                            namesAndDocstrings.append(('%s/%s' % (instance, viewName), docstring))
                    else:
                        mount_point = '/%s/%s' % (self.app.lower(), viewName)
                        viewObj = cherrypy.tree.apps[mount_point].root
                        docstring = viewObj.__doc__
                        namesAndDocstrings.append((viewName, docstring))
            cherrypy.tree.mount(Welcome(namesAndDocstrings), "/%s" % self.app.lower())

    def start(self, blocking=True):
        """
        Configure and start the server
        """
        self._validateConfig()
        self._configureCherryPy()
        self._loadPages()
        self._makeIndex()
        cherrypy.server.httpserver = None
        cherrypy.engine.start()
        if blocking:
            cherrypy.engine.block()

    def startComponent(self):
        """
        _startComponent_

        Called by the WMAgent harness code.  This will never return.
        """
        self.start()

    def stop(self):
        """
        Stop the server
        """
        cherrypy.engine.exit()
        cherrypy.engine.stop()

        # Ensure the next server that's started gets fresh objects
        for name, server in listitems(getattr(cherrypy, 'servers', {})):
            server.unsubscribe()
            del cherrypy.servers[name]


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-i", "--ini", dest="inifile", default=False,
                        help="write the configuration to FILE", metavar="FILE")
    parser.add_argument("-v", "--verbose",
                        action="store_true", dest="verbose", default=False,
                        help="Be more verbose")
    parser.add_argument("-d", "--daemonise",
                        action="store_true", dest="daemon", default=False,
                        help="Daemonise the cherrypy process, and return the PID")
    parser.add_argument("-s", "--status",
                        action="store_true", dest="status", default=False,
                        help="Return the status of the daemon")
    parser.add_argument("-k", "--kill",
                        action="store_true", dest="kill", default=False,
                        help="Kill the daemon")
    parser.add_argument("-t", "--terminate",
                        action="store_true", dest="terminate", default=False,
                        help="Terminate the daemon (kill, wait, kill -9)")
    opts = parser.parse_args()

    if not opts.inifile:
        sys.exit('No configuration specified')
    cfg = loadConfigurationFile(opts.inifile)

    component = cfg.Webtools.application
    workdir = getattr(cfg.Webtools, 'componentDir', '/tmp/webtools')
    if workdir is None:
        workdir = '/tmp/webtools'
    root = Root(cfg)
    if opts.status:
        daemon = Details('%s/Daemon.xml' % workdir)

        if not daemon.isAlive():
            print("Component:%s Not Running" % component)
        else:
            print("Component:%s Running:%s" % (component, daemon['ProcessID']))
    elif opts.kill:
        daemon = Details('%s/Daemon.xml' % workdir)
        daemon.kill()
        daemon.removeAndBackupDaemonFile()
    elif opts.terminate:
        daemon = Details('%s/Daemon.xml' % workdir)
        daemon.killWithPrejudice()
        daemon.removeAndBackupDaemonFile()
    elif opts.daemon:
        createDaemon(workdir)
        root.start(False)
    else:
        root.start()
