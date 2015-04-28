"""
Component of WMAgent that runs periodic checks of various runtime
metrics on the WMAgent host machine. Alerts are send out if soft, resp.
critical thresholds of particular metrics are exceeded.

"soft" metric's threshold - "soft" (buffered) Alert
"critical" metric's threshold - "critical" (i.e. non-buffered) Alert
soft, vs critical Alert is distinguished by setting Level attribute of the Alert
instance and it's up to AlertGenerator configuration which levels are considered below
soft, resp. below critical.

Checked metrics:
    as defined in the configuration sections (see this class test file for complete list)

"""



import os
import sys
import logging
import signal
import traceback

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



class AlertGenerator(Harness):
    """
    Agent's component to manage running of various configurable pollers.
    Each poller runs on background as separate process, pollers poll()
    method is entry point.

    Due to the logging mess in Harness, nothing, be it logging
    or print from here appears in the log files. Important stuff to
    move into preInitialization (from where logging works normally).

    """
    def __init__(self, config):
        Harness.__init__(self, config)
        self.config = config
        # poller instances (threads)
        self._pollers = []
        #3602 related:
        # Harness, nor the components, handle signal.SIGTERM which
        # is used by wmcoreD --shutdown, hence shutdown sequence is not called
        # this shall later be moved into (hopefully largely improved) Harness
        signal.signal(signal.SIGTERM, self._signalHandler)


    def _signalHandler(self, signalNumber, frame):
        logging.info("Signal number %s caught." % signalNumber)
        self.prepareToStop()


    def _createPollers(self):
        """
        Iterate over sections and instantiate corresponding Poller instances.

        """
        configuredPollers = []
        for poller in self.config.AlertGenerator.listSections_():
            if configSectionsToPollersMap.has_key(poller):
                pollerConf = getattr(self.config.AlertGenerator, poller)
                configuredPollers.append(poller)
                pollerClass = configSectionsToPollersMap[poller]
                logging.info("Instantiating %s (for '%s') ..." % (pollerClass, poller))
                try:
                    pollerObj = pollerClass(pollerConf, self)
                    self._pollers.append(pollerObj)
                    logging.info("%s initialized." % pollerObj.__class__.__name__)
                except Exception as ex:
                    trace = traceback.format_exception(*sys.exc_info())
                    traceString = '\n '.join(trace)
                    logging.error("%s failed to initialize, reason: %s\n%s" %
                                  (pollerClass, ex, traceString))

        l = [configSectionsToPollersMap.values(), configuredPollers, self._pollers]
        logging.info("Known pollers implementations:\n%s (%s)\n"
                     "Configured pollers:\n%s (%s)\n"
                     "Instantiated pollers:\n%s (%s)" %
                     (l[0], len(l[0]), l[1], len(l[1]), l[2], len(l[2])))


    def preInitialization(self):
        """
        Create poller instances running in threads.

        """
        logging.info("preInitialization - instantiating pollers ...")
        self._createPollers()
        logging.info("preInitialization - starting pollers ...")
        [poller.start() for poller in self._pollers]
        logging.info("preInitialization - finished.")


    def stopAlertGenerator(self):
        """
        Method to shutdown the AlertGenerator - stop all poller threads.

        """
        logging.info("stopAlertGenerator - stopping %s pollers ..." % len(self._pollers))
        [poller.stop() for poller in self._pollers]
        logging.info("stopAlertGenerator - terminating %s pollers threads ..." %
                      len(self._pollers))
        counter = 0
        for poller in self._pollers:
            logging.info("Terminating %s ..." % poller)
            poller.terminate()
            logging.info("Terminated: %s" % poller)
            counter += 1
        logging.info("stopAlertGenerator - finished, %s poller threads terminated." % counter)


    def prepareToStop(self, wait = False, stopPayload = ""):
        """
        Override prepareToStop to include call to stopAlertGenerator.
        Ugly, but seems no other way to do this...

        """
        logging.info("Shutting down the component - prepareToStop ...")
        self.stopAlertGenerator()
        Harness.prepareToStop(self, wait, stopPayload)
        logging.info("prepareToStop finished.")
