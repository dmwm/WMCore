"""
Component of WMAgent that runs periodic checks of various runtime
metrics on the WMAgent host machine. Alerts are send out if soft, resp.
critical thresholds of particular metrics are exceeded.

"soft" metric's threshold - "soft" (buffered) Alert
"critical" metric's threshold - "critical" (i.e. non-buffered) Alert
soft, vs critical Alert is distinguished by setting Level attribute of the Alert
instance and it's up to AlertProcessor configuration which levels are considered below
soft, resp. below critical. These AlertProcessor conf values are taken here.

Checked metrics:
    as defined in the configuration sections (see this class test file for complete list)
            
"""



import os
import time
import logging
from multiprocessing import Process

from WMCore.Agent.Harness import Harness

from WMComponent.AlertGenerator.Pollers.System import CPUPoller
from WMComponent.AlertGenerator.Pollers.System import MemoryPoller
from WMComponent.AlertGenerator.Pollers.System import DiskSpacePoller
from WMComponent.AlertGenerator.Pollers.Agent import ComponentsCPUPoller
from WMComponent.AlertGenerator.Pollers.Agent import ComponentsMemoryPoller
from WMComponent.AlertGenerator.Pollers.MySQL import MySQLCPUPoller
from WMComponent.AlertGenerator.Pollers.MySQL import MySQLMemoryPoller
from WMComponent.AlertGenerator.Pollers.MySQL import MySQLDbSizePoller
from WMComponent.AlertGenerator.Pollers.Couch import CouchDbSizePoller
from WMComponent.AlertGenerator.Pollers.Couch import CouchCPUPoller
from WMComponent.AlertGenerator.Pollers.Couch import CouchMemoryPoller
from WMComponent.AlertGenerator.Pollers.Couch import CouchErrorsPoller


        
configSectionsToPollersMap = {"cpuPoller": CPUPoller,
                              "memPoller": MemoryPoller,
                              "diskSpacePoller": DiskSpacePoller,
                              "componentsCPUPoller": ComponentsCPUPoller,
                              "componentsMemPoller": ComponentsMemoryPoller,
                              "mysqlCPUPoller": MySQLCPUPoller,
                              "mysqlMemPoller": MySQLMemoryPoller,
                              "mysqlDbSizePoller": MySQLDbSizePoller,
                              "couchDbSizePoller": CouchDbSizePoller,
                              "couchCPUPoller": CouchCPUPoller,
                              "couchMemPoller": CouchMemoryPoller,
                              "couchErrorsPoller": CouchErrorsPoller}



# TODO
#     clarify why .info level does not appear, .error does
class AlertGenerator(Harness):
    """
    Agent's component to manage running of various configurable pollers.
    Each poller runs on background as separate process, pollers poll()
    method is entry point.
    
    """
    def __init__(self, config):
        Harness.__init__(self, config)
        logging.info("%s initializing ... " % self.__class__.__name__)
        self.config = config                
        # poller instances, its poll() will run as a background process
        self._pollers = []
        # polling processes
        self._procs = []
        self._createPollers()
        logging.info("%s initialized." % self.__class__.__name__)
        
        
    def _createPollers(self):
        """
        Iterate over sections and instantiate corresponding Poller instances.
        
        """
        pollerMap = configSectionsToPollersMap
        for poller in self.config.AlertGenerator.listSections_():
            if pollerMap.has_key(poller):
                pollerConf = getattr(self.config.AlertGenerator, poller)
                pollerObj = pollerMap[poller](pollerConf, self)
                self._pollers.append(pollerObj)

        
    def preInitialization(self):
        """
        Start up the ZMQ Receiver + Processor.
        
        """
        logging.info("%s starting poller processes ..." % self.__class__.__name__)
        for poller in self._pollers:
            proc = Process(target = poller.poll, args = ())
            proc.start()
            self._procs.append(proc)
        
        
    def stopProcessor(self):
        """
        Method to shutdown the Alert Processor.
        
        """
        # stop all poller background processes spawned from here
        for proc, poller in zip(self._procs, self._pollers):
            poller.shutdown()
            proc.terminate()
        
        
    def prepareToStop(self, wait = False, stopPayload = ""):
        """
        Override prepareToStop to include call to stopProcessor.
        Ugly, but seems no other way to do this...
        
        """
        self.stopProcessor()
        Harness.prepareToStop(self, wait, stopPayload)