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
            cert, key = self.getCredentials()
            # if not provided in config search for credentials
            if cert and key:
                config.setdefault('key_file', key)
                config.setdefault('cert_file', cert)

            logger = get_logger(False, None)
            config.setdefault('logger', logger) #myThread.logger)
            myThread.workerThreadManager.addWorker(CouchProxyRunner(**config))

    def getCredentials(self):
        """Get x509 credentials, return cert & key"""
        if os.environ.get('X509_USER_PROXY'):
            return os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY']
        # cert will need to be unencryped
        elif os.environ.get('X509_USER_CERT'):
            if not os.environ.get('X509_USER_KEY'):
                raise RuntimeError, "X509_USER_CERT also requires X509_USER_KEY to be defined"
            return os.environ['X509_USER_CERT'], os.environ['X509_USER_KEY']
        return None, None

class CouchProxyRunner(BaseWorkerThread):
    """Actually run the proxy"""
    def __init__(self, **cfg):
        BaseWorkerThread.__init__(self)
        self.proxy = CouchProxy(**cfg)

    def algorithm(self, params = None):
        """Run in blocking mode.
        Does this give the heartbeat stuff a problem?"""
        self.proxy.run()