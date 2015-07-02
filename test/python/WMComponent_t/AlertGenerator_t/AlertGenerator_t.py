import os
import sys
import time
import unittest
import logging
import shutil
import types
import inspect

import psutil

from WMQuality.TestInit import TestInit
from WMCore.Configuration import Configuration
from WMComponent.AlertGenerator.AlertGenerator import AlertGenerator
from WMComponent_t.AlertGenerator_t.Pollers_t import utils
# poller final implementations
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


"""
Any new end (final) implementation of new poller(s) should be add
here to test its basic flow chain.

"""
finalPollerClasses = [CouchErrorsPoller,
                      CouchMemoryPoller,
                      CouchCPUPoller,
                      CouchDbSizePoller,
                      MySQLDbSizePoller,
                      MySQLMemoryPoller,
                      MySQLCPUPoller,
                      ComponentsMemoryPoller,
                      ComponentsCPUPoller,
                      DiskSpacePoller,
                      MemoryPoller,
                      CPUPoller]


def getConfig(testDir):
    periodAlertGeneratorPollers = 40 # [second]
    config = Configuration()
    config.section_("Agent")
    config.Agent.useMsgService = False
    config.Agent.useTrigger = False
    config.Agent.hostName = "localhost"
    config.Agent.teamName = "team1,team2,cmsdataops"
    config.Agent.agentName = "WMAgentCommissioning"

    # AlertProcessor values - values for Level soft, resp. critical
    # are also needed by this AlertGenerator test
    config.component_("AlertProcessor")
    config.AlertProcessor.componentDir = testDir
    config.AlertProcessor.section_("critical")
    config.AlertProcessor.section_("soft")
    config.AlertProcessor.critical.level = 5
    config.AlertProcessor.soft.level = 1

    # common 'Alert' section
    config.section_("Alert")
    # destination for the alert messages
    config.Alert.address = "tcp://127.0.0.1:6557"
    # control channel (internal alerts system commands)
    config.Alert.controlAddr = "tcp://127.0.0.1:6559"

    config.component_("AlertGenerator")
    config.AlertGenerator.componentDir = testDir
    config.AlertGenerator.logLevel     = 'DEBUG'
    # configuration for overall machine load monitor: cpuPoller (percentage values)
    config.AlertGenerator.section_("cpuPoller")
    config.AlertGenerator.cpuPoller.soft = 70 # [percent]
    config.AlertGenerator.cpuPoller.critical = 90 # [percent]
    config.AlertGenerator.cpuPoller.pollInterval = 10 # [second]
    # period during which measurements are collected before evaluating for possible alert triggering
    config.AlertGenerator.cpuPoller.period = periodAlertGeneratorPollers # [second]
    # configuration for overall used physical memory monitor: memPoller (percentage of total physical memory)
    config.AlertGenerator.section_("memPoller")
    config.AlertGenerator.memPoller.soft = 70 # [percent]
    config.AlertGenerator.memPoller.critical = 90 # [percent]
    config.AlertGenerator.memPoller.pollInterval = 10 # [second]
    # period during which measurements are collected before evaluating for possible alert triggering
    config.AlertGenerator.memPoller.period = periodAlertGeneratorPollers # [second]
    # configuration for available disk space monitor: diskSpacePoller (percentage usage per partition)
    config.AlertGenerator.section_("diskSpacePoller")
    config.AlertGenerator.diskSpacePoller.soft = 70 # [percent]
    config.AlertGenerator.diskSpacePoller.critical = 90 # [percent]
    config.AlertGenerator.diskSpacePoller.pollInterval = 10 # [second]
    # configuration for particular components CPU usage: componentCPUPoller (percentage values)
    config.AlertGenerator.section_("componentsCPUPoller")
    config.AlertGenerator.componentsCPUPoller.soft = 40 # [percent]
    config.AlertGenerator.componentsCPUPoller.critical = 60 # [percent]
    config.AlertGenerator.componentsCPUPoller.pollInterval = 10 # [second]
    # period during which measurements are collected before evaluating for possible alert triggering
    config.AlertGenerator.componentsCPUPoller.period = periodAlertGeneratorPollers # [second]
    # configuration for particular components memory monitor: componentMemPoller (percentage of total physical memory)
    config.AlertGenerator.section_("componentsMemPoller")
    config.AlertGenerator.componentsMemPoller.soft = 40 # [percent]
    config.AlertGenerator.componentsMemPoller.critical = 60 # [percent]
    config.AlertGenerator.componentsMemPoller.pollInterval = 10  # [second]
    # period during which measurements are collected before evaluating for possible alert triggering
    config.AlertGenerator.componentsMemPoller.period = periodAlertGeneratorPollers # [second]
    # configuration for MySQL server CPU monitor: mysqlCPUPoller (percentage values)
    config.AlertGenerator.section_("mysqlCPUPoller")
    config.AlertGenerator.mysqlCPUPoller.soft = 40 # [percent]
    config.AlertGenerator.mysqlCPUPoller.critical = 60 # [percent]
    config.AlertGenerator.mysqlCPUPoller.pollInterval = 10 # [second]
    # period during which measurements are collected before evaluating for possible alert triggering
    config.AlertGenerator.mysqlCPUPoller.period = periodAlertGeneratorPollers # [second]
    # configuration for MySQL memory monitor: mysqlMemPoller (percentage values)
    config.AlertGenerator.section_("mysqlMemPoller")
    config.AlertGenerator.mysqlMemPoller.soft = 40 # [percent]
    config.AlertGenerator.mysqlMemPoller.critical = 60 # [percent]
    config.AlertGenerator.mysqlMemPoller.pollInterval = 10 # [second]
    # period during which measurements are collected before evaluating for possible alert triggering
    config.AlertGenerator.mysqlMemPoller.period = periodAlertGeneratorPollers # [second]
    # configuration for MySQL database size: mysqlDbSizePoller (gigabytes values)
    config.AlertGenerator.section_("mysqlDbSizePoller")
    config.AlertGenerator.mysqlDbSizePoller.soft = 1 # GB
    config.AlertGenerator.mysqlDbSizePoller.critical = 2 # GB
    config.AlertGenerator.mysqlDbSizePoller.pollInterval = 10 # [second]
    # configuration for CouchDB database size monitor: couchDbSizePoller (gigabytes values)
    config.AlertGenerator.section_("couchDbSizePoller")
    config.AlertGenerator.couchDbSizePoller.couchURL = os.getenv("COUCHURL", None)
    config.AlertGenerator.couchDbSizePoller.soft = 1 # GB
    config.AlertGenerator.couchDbSizePoller.critical = 2 # GB
    config.AlertGenerator.couchDbSizePoller.pollInterval = 10 # [second]
    # configuration for CouchDB CPU monitor: couchCPUPoller (percentage values)
    config.AlertGenerator.section_("couchCPUPoller")
    config.AlertGenerator.couchCPUPoller.couchURL = os.getenv("COUCHURL", None)
    config.AlertGenerator.couchCPUPoller.soft = 40 # [percent]
    config.AlertGenerator.couchCPUPoller.critical = 60 # [percent]
    config.AlertGenerator.couchCPUPoller.pollInterval = 10 # [second]
    # period during which measurements are collected before evaluating for possible alert triggering
    config.AlertGenerator.couchCPUPoller.period = periodAlertGeneratorPollers # [second]
    # configuration for CouchDB memory monitor: couchMemPoller (percentage values)
    config.AlertGenerator.section_("couchMemPoller")
    config.AlertGenerator.couchMemPoller.couchURL = os.getenv("COUCHURL", None)
    config.AlertGenerator.couchMemPoller.soft = 40 # [percent]
    config.AlertGenerator.couchMemPoller.critical = 60 # [percent]
    config.AlertGenerator.couchMemPoller.pollInterval = 10 # [second]
    # period during which measurements are collected before evaluating for possible alert triggering
    config.AlertGenerator.couchMemPoller.period = periodAlertGeneratorPollers # [second]
    # configuration for CouchDB HTTP errors poller: couchErrorsPoller (number of error occurrences)
    # (once certain threshold of the HTTP error counters is exceeded, poller keeps sending alerts)
    config.AlertGenerator.section_("couchErrorsPoller")
    config.AlertGenerator.couchErrorsPoller.couchURL = os.getenv("COUCHURL", None)
    config.AlertGenerator.couchErrorsPoller.soft = 100 # [number of error occurrences]
    config.AlertGenerator.couchErrorsPoller.critical = 200 # [number of error occurrences]
    config.AlertGenerator.couchErrorsPoller.observables = (404, 500) # HTTP status codes to watch over
    config.AlertGenerator.couchErrorsPoller.pollInterval = 10 # [second]

    return config



class AlertGeneratorTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging(logLevel = logging.DEBUG)
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS",'WMCore.Agent.Database',
                                                 "WMCore.ResourceControl"],
                                useDefault = False)
        self.testDir = self.testInit.generateWorkDir()
        # AlertGenerator instance
        self.generator = None
        self.config = getConfig(self.testDir)
        self.config.section_("CoreDatabase")
        self.config.CoreDatabase.socket = os.environ.get("DBSOCK")
        self.config.CoreDatabase.connectUrl = os.environ.get("DATABASE")
        self.testComponentDaemonXml = os.path.join(self.testDir, "Daemon.xml")


    def tearDown(self):
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        self.generator = None
        # if the directory and file "/tmp/TestComponent/Daemon.xml" after
        # ComponentsPoller test exist, then delete it
        d = os.path.dirname(self.testComponentDaemonXml)
        if os.path.exists(d):
            shutil.rmtree(d)


    def _startComponent(self):
        self.generator = AlertGenerator(self.config)
        try:
            # self.proc.startComponent() causes the flow to stop, Harness.py
            # the method just calls prepareToStart() and waits for ever
            # self.proc.startDaemon() no good for this either ... puts everything
            # on background
            self.generator.prepareToStart() # is method of Harness
        except Exception as ex:
            print ex
            self.fail(str(ex))
        logging.debug("AlertGenerator and its sub-components should be running now ...")


    def _stopComponent(self):
        logging.debug("Going to stop the AlertGenerator ...")
        # stop via component method
        try:
            self.generator.stopAlertGenerator()
        except Exception as ex:
            logging.error(ex)
            self.fail(str(ex))
        logging.debug("AlertGenerator should be stopped now.")


    def testAlertProcessorBasic(self):
        """
        Just tests starting and stopping the component machinery.
        Should start and stop all configured pollers.

        """
        # the generator will run full-fledged pollers that may get triggered
        # to send some alerts. need to consume such in order to avoid clashes
        # further tests
        handler, receiver = utils.setUpReceiver(self.config.Alert.address,
                                                self.config.Alert.controlAddr)
        self._startComponent()
        # test that all poller processes are running
        for poller in self.generator._pollers:
            self.assertTrue(poller.is_alive())
        # just give the pollers some time to run
        time.sleep(5)
        self._stopComponent()
        receiver.shutdown()
        print "%s alerts captured by the way (test %s)." % (len(handler.queue),
                                                            inspect.stack()[0][3])


    def testAllFinalClassPollerImplementations(self):
        """
        Any new end (final) implementation of new poller(s) should be add
        here to test its basic flow chain.

        """
        config = getConfig("/tmp")
        # create some non-sence config section. just need a bunch of values defined
        config.AlertGenerator.section_("bogusPoller")
        # only couch-related pollers require couchURL, this way it'll be used at the
        # other ones as well, should do no harm ; it's just because all pollers are
        # probed here in a single test ...
        config.AlertGenerator.bogusPoller.couchURL = os.getenv("COUCHURL", None)
        config.AlertGenerator.bogusPoller.soft = 5 # [percent]
        config.AlertGenerator.bogusPoller.critical = 50 # [percent]
        config.AlertGenerator.bogusPoller.pollInterval = 0.2  # [second]
        config.AlertGenerator.bogusPoller.period = 0.5
        # currently only CouchErrorsPoller uses this config value
        config.AlertGenerator.bogusPoller.observables = 4000

        # need to create some temp directory, real process and it's
        # Daemon.xml so that is looks like agents component process
        # and check back the information, give its own PID
        pid = os.getpid()
        config.component_("TestComponent")
        d = os.path.dirname(self.testComponentDaemonXml)
        config.TestComponent.componentDir = d
        if not os.path.exists(d):
            os.mkdir(d)
        f = open(self.testComponentDaemonXml, 'w')
        f.write(utils.daemonXmlContent % dict(PID_TO_PUT = pid))
        f.close()

        generator = utils.AlertGeneratorMock(config)
        pollers = []
        for pollerClass in finalPollerClasses:
            p = pollerClass(config.AlertGenerator.bogusPoller, generator)
            # poller may send something during below check(), satisfy sender method
            p.sender = lambda alert: 1 + 1
            pollers.append(p)

        for poller in pollers:
            poller.check()
            if hasattr(poller, "_measurements"):
                mes = poller._measurements
                self.assertEqual(len(mes), 1)
                self.assertTrue(isinstance(mes[0], float))
            if hasattr(poller, "_compMeasurements"):
                for measurements in poller._compMeasurements:
                    self.assertEqual(len(measurements), 1)
                    self.assertTrue(isinstance(measurements[0], float))

        shutil.rmtree(d)

        # don't do shutdown() on poller - will take a while and it's not
        # necessary anyway - BasePoller.start() which does register is not
        # called here so the threads are not running in fact



if __name__ == "__main__":
    unittest.main()
