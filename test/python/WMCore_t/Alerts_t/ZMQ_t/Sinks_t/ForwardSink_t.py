import os
import time
import unittest
import inspect
import logging

from WMCore.Configuration import ConfigSection
from WMCore.Configuration import Configuration
from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Processor import Processor
from WMCore.Alerts.ZMQ.Sender import Sender
from WMCore.Alerts.ZMQ.Receiver import Receiver
from WMCore.Alerts.ZMQ.Sinks.ForwardSink import ForwardSink
from WMCore.Alerts.ZMQ.Sinks.FileSink import FileSink
from WMQuality.TestInit import TestInit



def worker(addr, ctrl, nAlerts, workerId = "ForwardSinkTestSource"):
    """
    Send a few alerts.

    """
    s = Sender(addr, ctrl, workerId)
    s.register()
    d = dict(very = "interesting")
    [s(Alert(Type = "Alert", Level = i, Details = d)) for i in range(0, nAlerts)]
    s.unregister()
    s.sendShutdown()



class ForwardSinkTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging(logLevel = logging.DEBUG)
        self.testDir = self.testInit.generateWorkDir()

        self.address1 = "tcp://127.0.0.1:5557"
        self.controlAddr1 = "tcp://127.0.0.1:5559"

        self.address2 = "tcp://127.0.0.1:15557"
        self.controlAddr2 = "tcp://127.0.0.1:15559"

        self.outputfileCritical = os.path.join(self.testDir, "ForwardSinkTestCritical.json")
        self.outputfileSoft = os.path.join(self.testDir, "ForwardSinkTestSoft.json")


    def tearDown(self):
        self.testInit.delWorkDir()


    def testForwardSinkBasic(self):
        config = ConfigSection("forward")
        # address of the Processor, resp. Receiver to forward Alerts to
        config.address = self.address1
        config.controlAddr = self.controlAddr1
        config.label = "ForwardSinkTest"
        forwarder = ForwardSink(config)


    def testForwardSinkEntireChain(self):
        """
        The test chain looks as follows:
        worker -> Receiver1(+its Processor configured to do ForwardSink) -> Receiver2 whose
            address as the destination the ForwardSink is configured with -> Receiver2 will
            do FileSink so that it's possible to verify the chain.

        """
        # configuration for the Receiver+Processor+ForwardSink 1 (group)
        config1 = Configuration()
        config1.component_("AlertProcessor")
        config1.AlertProcessor.section_("critical")
        config1.AlertProcessor.section_("soft")

        config1.AlertProcessor.critical.level = 5
        config1.AlertProcessor.soft.level = 0
        config1.AlertProcessor.soft.bufferSize = 0

        config1.AlertProcessor.critical.section_("sinks")
        config1.AlertProcessor.soft.section_("sinks")

        config1.AlertProcessor.critical.sinks.section_("forward")
        config1.AlertProcessor.soft.sinks.section_("forward")
        # address of the Receiver2
        config1.AlertProcessor.critical.sinks.forward.address = self.address2
        config1.AlertProcessor.critical.sinks.forward.controlAddr = self.controlAddr2
        config1.AlertProcessor.critical.sinks.forward.label = "ForwardSinkTest"
        config1.AlertProcessor.soft.sinks.forward.address = self.address2
        config1.AlertProcessor.soft.sinks.forward.controlAddr = self.controlAddr2
        config1.AlertProcessor.soft.sinks.forward.label = "ForwardSinkTest"

        # 1) first item of the chain is source of Alerts: worker()

        # 2) second item is Receiver1 + its Processor + its ForwardSink
        processor1 = Processor(config1.AlertProcessor)
        # ForwardSink will be created automatically by the Processor
        receiver1 = Receiver(self.address1, processor1, self.controlAddr1)
        receiver1.startReceiver() # non blocking call

        # 3) third group is Receiver2 with its Processor and final FileSink
        config2 = Configuration()
        config2.component_("AlertProcessor")
        config2.AlertProcessor.section_("critical")
        config2.AlertProcessor.section_("soft")

        config2.AlertProcessor.critical.level = 5
        config2.AlertProcessor.soft.level = 0
        config2.AlertProcessor.soft.bufferSize = 0

        config2.AlertProcessor.critical.section_("sinks")
        config2.AlertProcessor.soft.section_("sinks")

        config2.AlertProcessor.critical.sinks.section_("file")
        config2.AlertProcessor.soft.sinks.section_("file")
        # configuration of the final sink
        config2.AlertProcessor.critical.sinks.file.outputfile = self.outputfileCritical
        config2.AlertProcessor.soft.sinks.file.outputfile = self.outputfileSoft

        processor2 = Processor(config2.AlertProcessor)
        # final FileSink will be automatically created by the Processor
        receiver2 = Receiver(self.address2, processor2, self.controlAddr2)
        receiver2.startReceiver() # non blocking call

        # now send the Alert messages via worker() and eventually shut the receiver1
        worker(self.address1, self.controlAddr1, 10)
        # wait until receiver1 shuts
        while receiver1.isReady():
            time.sleep(0.4)
            print "%s waiting for Receiver1 to shut ..." % inspect.stack()[0][3]

        # shut down receiver2 - need to sendShutdown() to it
        s = Sender(self.address2, self.controlAddr2, "some_id")
        s.sendShutdown()
        # wait until receiver2 shuts
        while receiver2.isReady():
            time.sleep(0.4)
            print "%s waiting for Receiver2 to shut ..." % inspect.stack()[0][3]

        # check the result in the files
        # the bufferSize for soft-level Alerts was set to 0 so all
        # Alerts should be present also in the soft-level type file
        # initial 10 Alerts (Level 0 .. 9) gets distributed though a cascade
        # of two Receivers. soft alerts with level 0 .. 4 are considered
        # so Receiver1 forwards through its ForwardSink 0 .. 4 Alerts as soft and
        # 5 .. 9 level Alerts through 'critical'. order is not guaranteed
        # critical Alerts
        fileConfig = ConfigSection("file")
        fileConfig.outputfile = self.outputfileCritical
        sink = FileSink(fileConfig)
        expectedLevels = range(5, 10) # that is 5 .. 9
        loadAlerts = sink.load()
        self.assertEqual(len(loadAlerts), len(expectedLevels))
        d = dict(very = "interesting")
        for a in loadAlerts:
            self.assertEqual(a["Details"], d)

        # soft Alerts
        fileConfig = ConfigSection("file")
        fileConfig.outputfile = self.outputfileSoft
        sink = FileSink(fileConfig)
        expectedLevels = range(0, 5) # that is 0 .. 4
        loadAlerts = sink.load()
        self.assertEqual(len(loadAlerts), len(expectedLevels))
        for a in loadAlerts:
            self.assertEqual(a["Details"], d)



if __name__ == "__main__":
    unittest.main()
