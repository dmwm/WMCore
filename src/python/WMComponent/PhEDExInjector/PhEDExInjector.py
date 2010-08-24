
# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
# we do not import failure handlers as they are dynamicly 
# loaded from the config file.
from WMCore.WMFactory import WMFactory
import os
import threading
import logging
from WMCore.Configuration import loadConfigurationFile
from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory
from WMComponent.PhEDExInjector.PhEDExInjectorPoller import PhEDExInjectorPoller
#from WMCore.HTTPFrontEnd import Downloader
#from WMCore.WebTools.Root import Root

factory = WMFactory('generic')


class PhEDExInjector(Harness):
    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1
        #self.start()

    def preInitialization(self):
        print "PhEDExInjector.preInitialization"
        
        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        
        
        # Add event loop to worker manager
        myThread = threading.currentThread()
        
        pollInterval = self.config.PhEDExInjector.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(PhEDExInjectorPoller(self.config), pollInterval)
        