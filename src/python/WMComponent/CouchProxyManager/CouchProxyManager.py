#!/usr/bin/env python
"""
CouchProxyManager

Some couch setups (i.e. behind cmsweb) require the user to present x509 credentials.
This component can proxy inter-couchdb traffic (i.e. replications) adding in the
required certificate.

Multiple proxies are possible by adding multiple sections to the config
"""

import logging
import os
import threading
from CouchProxy import CouchProxy, get_logger

from WMCore.Agent.Harness import Harness
from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread

class CouchProxyManager(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        return

    def preInitialization(self):
        myThread = threading.currentThread()

        for section in self.config.section_('CouchProxyManager'):
            if not hasattr(section, 'pythonise_'):
                continue # not an actual section
            config = section.dictionary_()
            # add x509 if not set in config
            #TODO: Expand this to look for certificates also
            if os.environ.get('X509_USER_PROXY'):
                config.setdefault('key_file', os.environ.get('X509_USER_PROXY'))
                config.setdefault('cert_file', os.environ.get('X509_USER_PROXY'))

            logger = get_logger(False, None)
            config.setdefault('logger', logger) #myThread.logger)
            myThread.workerThreadManager.addWorker(CouchProxyRunner(**config))


class CouchProxyRunner(BaseWorkerThread):
    """Actually run the proxy"""
    def __init__(self, **cfg):
        BaseWorkerThread.__init__(self)
        self.proxy = CouchProxy(**cfg)

    def algorithm(self, params = None):
        """Run in blocking mode.
        Does this give the heartbeat stuff a problem?"""
        self.proxy.run()