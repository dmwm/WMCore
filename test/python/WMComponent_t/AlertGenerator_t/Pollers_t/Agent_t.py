"""
Modules contains tests for agent-related metrics, CPU/mem usage by
agent components, etc.

"""


import os
import unittest
import logging
import time
import random
import shutil
import datetime
import inspect

import psutil

from WMCore.Configuration import Configuration
from WMComponent.AlertGenerator.Pollers.Base import ProcessDetail
from WMComponent.AlertGenerator.Pollers.Base import Measurements
from WMComponent.AlertGenerator.Pollers.Agent import ComponentsPoller
from WMComponent.AlertGenerator.Pollers.Agent import ComponentsCPUPoller
from WMComponent.AlertGenerator.Pollers.Agent import ComponentsMemoryPoller
from WMQuality.TestInit import TestInit
from WMComponent_t.AlertGenerator_t.Pollers_t import utils
from WMComponent_t.AlertGenerator_t.AlertGenerator_t import getConfig



# full production WMAgent configuration file
configFile = os.environ.get("WMAGENT_CONFIG", None)
if configFile:
    execfile(configFile)



class AgentTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging(logLevel = logging.DEBUG)
        self.testDir = self.testInit.generateWorkDir()
        self.config = getConfig(self.testDir)
        # mock generator instance to communicate some configuration values
        self.generator = utils.AlertGeneratorMock(self.config)
        self.testComponentDaemonXml = os.path.join(self.testDir, "Daemon.xml")


    def tearDown(self):
        self.testInit.delWorkDir()
        self.generator = None


    def testComponentsPollerBasic(self):
        """
        Test ComponentsPoller class.
        Beware of different process context in real running.

        """
        config = getConfig("/tmp")
        config.component_("AlertProcessor")
        config.AlertProcessor.section_("critical")
        config.AlertProcessor.section_("soft")
        config.AlertProcessor.critical.level = 5
        config.AlertProcessor.soft.level = 0
        config.component_("AlertGenerator")
        config.AlertGenerator.section_("bogusPoller")
        config.AlertGenerator.bogusPoller.soft = 5 # [percent]
        config.AlertGenerator.bogusPoller.critical = 90 # [percent]
        config.AlertGenerator.bogusPoller.pollInterval = 2  # [second]
        # period during which measurements are collected before evaluating for possible alert triggering
        config.AlertGenerator.bogusPoller.period = 10

        # need to create some temp directory, real process and it's
        # Daemon.xml so that is looks like agents component process
        # and check back the information
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
        poller = ComponentsPoller(config.AlertGenerator.bogusPoller, generator)

        # only 1 component should have valid workDir with proper Daemon.xml content
        # other components present in the configuration (AlertProcessor, AlertGenerator)
        # should have been ignored
        self.assertEqual(len(poller._components), 1)
        pd = poller._components[0]
        self.assertEqual(pd.pid, pid)
        self.assertEqual(pd.name, "TestComponent")
        self.assertEqual(len(poller._compMeasurements), 1)
        mes = poller._compMeasurements[0]
        numMeasurements = round(config.AlertGenerator.bogusPoller.period / config.AlertGenerator.bogusPoller.pollInterval, 0)
        self.assertEqual(mes._numOfMeasurements, numMeasurements)

        shutil.rmtree(d)


    def _doComponentsPoller(self, thresholdToTest, level, config,
                            pollerClass, expected = 0):
        """
        Components pollers have array of Measurements and ProcessDetails
        which make it more difficult to factory with test methods from the
        utils module.

        """
        handler, receiver = utils.setUpReceiver(self.generator.config.Alert.address,
                                                self.generator.config.Alert.controlAddr)

        # need some real process to poll, give itself
        pid = os.getpid()
        # the input configuration doesn't have component work directories set right, rectify:
        # the configuration will have, see with what _doComponentsPoller is called
        # two components: AlertGenerator and AlertProcessor defined
        configInstance = Configuration.getInstance()
        for comp in Configuration.getInstance().listComponents_():
            compDir = getattr(configInstance, comp).componentDir
            compDir = os.path.join(compDir, comp)
            setattr(getattr(configInstance, comp), "componentDir", compDir)
            os.makedirs(compDir)
            f = open(os.path.join(compDir, "Daemon.xml"), 'w')
            f.write(utils.daemonXmlContent % dict(PID_TO_PUT = pid))
            f.close()

        numMeasurements = config.period / config.pollInterval
        poller = pollerClass(config, self.generator)
        # inject own input sample data provider
        # there is in fact input argument in this case which needs be ignored
        poller.sample = lambda proc_: random.randint(thresholdToTest, thresholdToTest + 10)

        # the poller will run upon components (as defined in the configuration)
        # and poll them. the PID, etc will be run from the compomentsDir
        poller.start()
        self.assertTrue(poller.is_alive())

        if expected != 0:
            # watch so that the test can't take for ever, fail in 2mins
            timeLimitExceeded = False
            startTime = datetime.datetime.now()
            limitTime = 2 * 60 # seconds
            while len(handler.queue) == 0:
                time.sleep(config.pollInterval / 5)
                if (datetime.datetime.now() - startTime).seconds > limitTime:
                    timeLimitExceeded = True
                    break
        else:
            time.sleep(config.period * 2)

        poller.terminate()
        receiver.shutdown()
        self.assertFalse(poller.is_alive())

        if expected != 0:
            if timeLimitExceeded:
                self.fail("No alert received in %s seconds." % limitTime)
            # there should be just one alert received, poller should have the
            # change to send a second
            self.assertEqual(len(handler.queue), expected)
            a = handler.queue[0]
            # soft threshold - alert should have 'soft' level
            self.assertEqual(a["Level"], level)
            self.assertEqual(a["Component"], self.generator.__class__.__name__)
            self.assertEqual(a["Source"], poller.__class__.__name__)


    def testComponentsCPUPollerSoftThreshold(self):
        self.config.AlertGenerator.componentsCPUPoller.soft = 70
        self.config.AlertGenerator.componentsCPUPoller.critical = 80
        self.config.AlertGenerator.componentsCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.componentsCPUPoller.period = 1
        level = self.config.AlertProcessor.soft.level
        thresholdToTest = self.config.AlertGenerator.componentsCPUPoller.soft
        # expected 2: 2 components are defined by the configuration, an alert
        # will be sent for each of them
        self._doComponentsPoller(thresholdToTest, level,
                                 self.config.AlertGenerator.componentsCPUPoller,
                                 ComponentsCPUPoller, expected = 2)


    def testComponentsCPUPollerCriticalThreshold(self):
        self.config.AlertGenerator.componentsCPUPoller.soft = 70
        self.config.AlertGenerator.componentsCPUPoller.critical = 80
        self.config.AlertGenerator.componentsCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.componentsCPUPoller.period = 1
        level = self.config.AlertProcessor.critical.level
        thresholdToTest = self.config.AlertGenerator.componentsCPUPoller.critical
        # expected 2: 2 components are defined by the configuration, an alert
        # will be sent for each of them
        self._doComponentsPoller(thresholdToTest, level,
                                 self.config.AlertGenerator.componentsCPUPoller,
                                 ComponentsCPUPoller, expected = 2)


    def testComponentsCPUPollerNoAlert(self):
        self.config.AlertGenerator.componentsCPUPoller.soft = 70
        self.config.AlertGenerator.componentsCPUPoller.critical = 80
        self.config.AlertGenerator.componentsCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.componentsCPUPoller.period = 1
        level = 0
        # lower the threshold so that the alert never happens
        thresholdToTest = self.config.AlertGenerator.componentsCPUPoller.soft - 10
        self._doComponentsPoller(thresholdToTest, level,
                                 self.config.AlertGenerator.componentsCPUPoller,
                                 ComponentsCPUPoller, expected = 0)


    def testComponentsMemoryPollerSoftThreshold(self):
        self.config.AlertGenerator.componentsMemPoller.soft = 70
        self.config.AlertGenerator.componentsMemPoller.critical = 80
        self.config.AlertGenerator.componentsMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.componentsMemPoller.period = 1
        level = self.config.AlertProcessor.soft.level
        thresholdToTest = self.config.AlertGenerator.componentsMemPoller.soft
        # expected 2: 2 components are defined by the configuration, an alert
        # will be sent for each of them
        self._doComponentsPoller(thresholdToTest, level,
                                 self.config.AlertGenerator.componentsMemPoller,
                                 ComponentsMemoryPoller, expected = 2)


    def testComponentsMemoryPollerCriticalThreshold(self):
        self.config.AlertGenerator.componentsMemPoller.soft = 70
        self.config.AlertGenerator.componentsMemPoller.critical = 80
        self.config.AlertGenerator.componentsMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.componentsMemPoller.period = 1
        level = self.config.AlertProcessor.critical.level
        thresholdToTest = self.config.AlertGenerator.componentsMemPoller.critical
        # expected 2: 2 components are defined by the configuration, an alert
        # will be sent for each of them
        self._doComponentsPoller(thresholdToTest, level,
                                 self.config.AlertGenerator.componentsMemPoller,
                                 ComponentsMemoryPoller, expected = 2)


    def testComponentsMemoryPollerNoAlert(self):
        self.config.AlertGenerator.componentsMemPoller.soft = 70
        self.config.AlertGenerator.componentsMemPoller.critical = 80
        self.config.AlertGenerator.componentsMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.componentsMemPoller.period = 1
        level = 0
        # lower the threshold so that the alert never happens
        thresholdToTest = self.config.AlertGenerator.componentsCPUPoller.soft - 10
        self._doComponentsPoller(thresholdToTest, level,
                                 self.config.AlertGenerator.componentsMemPoller,
                                 ComponentsMemoryPoller, expected = 0)


    def testComponentsCPUPollerPossiblyOnLiveAgent(self):
        """
        If there is currently running agent upon WMAGENT_CONFIG
        configuration, then the test will pick up live processes
        and poll them.

        """
        # check if the live agent configuration was loaded (above this class)
        if globals().has_key("config"):
            self.config = config
            # AlertProcessor values - values for Level soft, resp. critical
            # are also needed by this AlertGenerator test
            self.config.component_("AlertProcessor")
            self.config.AlertProcessor.componentDir = "/tmp"
            self.config.AlertProcessor.section_("critical")
            self.config.AlertProcessor.section_("soft")
            self.config.AlertProcessor.critical.level = 5
            self.config.AlertProcessor.soft.level = 0

            self.config.component_("AlertGenerator")
            self.config.AlertGenerator.componentDir = "/tmp"
            self.config.section_("Alert")
            self.config.Alert.address = "tcp://127.0.0.1:6557"
            self.config.Alert.controlAddr = "tcp://127.0.0.1:6559"

            self.config.AlertGenerator.section_("componentsCPUPoller")
        else:
            self.config = getConfig("/tmp")

        self.config.AlertGenerator.componentsCPUPoller.soft = 70
        self.config.AlertGenerator.componentsCPUPoller.critical = 80
        self.config.AlertGenerator.componentsCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.componentsCPUPoller.period = 0.3

        # generator has already been instantiated, but need another one
        # with just defined configuration
        # mock generator instance to communicate some configuration values
        self.generator = utils.AlertGeneratorMock(self.config)

        handler, receiver = utils.setUpReceiver(self.generator.config.Alert.address,
                                                self.generator.config.Alert.controlAddr)

        numMeasurements = self.config.AlertGenerator.componentsCPUPoller.period / self.config.AlertGenerator.componentsCPUPoller.pollInterval
        poller = ComponentsCPUPoller(self.config.AlertGenerator.componentsCPUPoller, self.generator)
        # inject own input sample data provider
        thresholdToTest = self.config.AlertGenerator.componentsCPUPoller.soft
        # there is in fact input argument in this case which needs be ignored
        poller.sample = lambda proc_: random.randint(thresholdToTest - 10, thresholdToTest)

        poller.start()
        self.assertTrue(poller.is_alive())

        # no alert shall arrive
        time.sleep(5 * self.config.AlertGenerator.componentsCPUPoller.period)

        poller.terminate()
        receiver.shutdown()
        self.assertFalse(poller.is_alive())



if __name__ == "__main__":
    unittest.main()
