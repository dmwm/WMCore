import os
import unittest
import logging
import time

from WMQuality.TestInit import TestInit
from WMCore.Configuration import Configuration
from WMComponent.AlertProcessor.AlertProcessor import AlertProcessor



class AlertProcessorTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging(logLevel = logging.DEBUG)
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS",'WMCore.Agent.Database',
                                                 "WMCore.ResourceControl"],
                                 useDefault = False)
        self.testDir = self.testInit.generateWorkDir()

        self.config = Configuration()
        self.config.section_("Agent")
        self.config.Agent.useMsgService = False
        self.config.Agent.useTrigger = False
        self.config.component_("AlertProcessor")
        self.config.AlertProcessor.componentDir = self.testDir
        self.config.AlertProcessor.address = "tcp://127.0.0.1:5557"
        self.config.AlertProcessor.controlAddr = "tcp://127.0.0.1:5559"
        self.config.section_("CoreDatabase")

        self.config.CoreDatabase.socket = os.environ.get("DBSOCK")
        self.config.CoreDatabase.connectUrl = os.environ.get("DATABASE")

        self.config.AlertProcessor.section_("critical")
        self.config.AlertProcessor.section_("soft")

        self.config.AlertProcessor.critical.level = 5
        self.config.AlertProcessor.soft.level = 0
        self.config.AlertProcessor.soft.bufferSize = 3

        self.config.AlertProcessor.critical.section_("sinks")
        self.config.AlertProcessor.soft.section_("sinks")


    def tearDown(self):
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()


    def testAlertProcessorBasic(self):
        alertProcessor = AlertProcessor(self.config)
        try:
            # alertProcessor.startComponent() causes the flow to stop, Harness.py
            # the method just calls prepareToStart() and waits for ever
            # alertProcessor.startDaemon() no good for this either ... puts everything
            # on background
            alertProcessor.prepareToStart()
        except Exception as ex:
            print ex
            self.fail(str(ex))

        logging.debug("AlertProcessor and its sub-components should be running now ...")
        logging.debug("Going to stop the component ...")

        # stop via component method
        try:
            alertProcessor.stopAlertProcessor()
        except Exception as ex:
            print ex
            self.fail(str(ex))



if __name__ == "__main__":
    unittest.main()
