import os
import time
import logging
import unittest
import inspect

from WMCore.Alerts.Alert import Alert
from WMCore.Configuration import Configuration
from WMCore.Alerts.ZMQ.Processor import Processor
from WMCore.Alerts.ZMQ.Sender import Sender
from WMCore.Alerts.ZMQ.Receiver import Receiver, ReceiverLogic
from WMCore.Alerts.ZMQ.Sinks.FileSink import FileSink
from WMQuality.TestInitCouchApp import TestInitCouchApp



def worker(addr, ctrl, nAlerts, workerId = "Processor_t"):
    """
    Instantiate an alert Sender instance and register with Received instance
    identified by addr (alerts channel), ctrl (control channel) addresses.
    Then send a desired amount of alerts, unregister and send Shutdown control
    message instructing the Receive to stop and release sockets.

    """
    s = Sender(addr, ctrl, workerId)
    s.register()
    for i in range(0, nAlerts):
        a = Alert(Type = "Alert", Level = i)
        s(a)
    s.unregister()
    s.sendShutdown()



class ProcessorTest(unittest.TestCase):
    """
    TestCase for Processor.

    """


    def setUp(self):
        """
        Set up for tests.

        """

        l = logging.getLogger()
        l.setLevel(logging.DEBUG)

        self.addr = "tcp://127.0.0.1:5557"
        self.ctrl = "tcp://127.0.0.1:5559"

        self.softOutputFile = "/tmp/ProcessorTestSoftAlerts.json"
        self.criticalOutputFile = "/tmp/ProcessorTestCriticalAlerts.json"

        self.config = Configuration()
        self.config.component_("AlertProcessor")
        self.config.AlertProcessor.section_("critical")
        self.config.AlertProcessor.section_("soft")

        self.config.AlertProcessor.critical.level = 5
        self.config.AlertProcessor.soft.level = 1
        self.config.AlertProcessor.soft.bufferSize = 3

        self.config.AlertProcessor.critical.section_("sinks")
        self.config.AlertProcessor.soft.section_("sinks")


    def tearDown(self):
        for f in (self.criticalOutputFile, self.softOutputFile):
            if os.path.exists(f):
                os.remove(f)
        if hasattr(self, "testInit"):
            self.testInit.tearDownCouch()
        if hasattr(self, "receiver"):
            # wait until the Receiver is shut by the Shutdown control
            # message which the worker() function should have sent
            while self.receiver.isReady():
                logging.info("tearDown: Waiting for Receiver shutdown ...")
                time.sleep(ReceiverLogic.TIMEOUT_AFTER_SHUTDOWN * 1.1)
                if self.receiver.isReady():
                    self.receiver.shutdown()
            logging.info("tearDown: Is the Receiver shut down: %s" % (not self.receiver.isReady()))


    def testProcessorBasic(self):
        str(self.config.AlertProcessor)
        p = Processor(self.config.AlertProcessor)


    def testProcessorWithReceiver(self):
        """
        Test startup and shutdown of processor in receiver.

        """
        processor = Processor(self.config.AlertProcessor)
        # Receiver is waited for shutdown / shutdown explicitly in tearDown()
        self.receiver = Receiver(self.addr, processor, self.ctrl)
        self.receiver.startReceiver() # non-blocking call

        # now sender tests control messages (register, unregister, shutdown)
        s = Sender(self.addr, self.ctrl, "Processor_t")
        s.register()
        s.unregister()
        s.sendShutdown()
        # give some time so that the previous call shuts down the receiver
        time.sleep(ReceiverLogic.TIMEOUT_AFTER_SHUTDOWN * 1.1)


    def testProcessorWithReceiverAndFileSink(self):
        # add corresponding part of the configuration for FileSink(s)
        config = self.config.AlertProcessor
        config.critical.sinks.section_("file")
        config.critical.sinks.file.outputfile = self.criticalOutputFile

        config.soft.sinks.section_("file")
        config.soft.sinks.file.outputfile = self.softOutputFile

        processor = Processor(config)
        # Receiver is waited for shutdown / shutdown explicitly in tearDown()
        self.receiver = Receiver(self.addr, processor, self.ctrl)
        self.receiver.startReceiver() # non blocking call

        # run worker(), this time directly without Process as above,
        # worker will send 10 Alerts to Receiver
        worker(self.addr, self.ctrl, 10)

        # wait until Receiver is shut down (by a message from worker(), then all
        # alerts shall be delivered and could proceed to check if successfully delivered
        while self.receiver.isReady():
            time.sleep(ReceiverLogic.TIMEOUT_AFTER_SHUTDOWN * 1.5)
            logging.info("%s: Waiting for Receiver shutdown ..." % inspect.stack()[0][3])

        # check the FileSink output files for content:
        # the soft Alerts has threshold level set to 0 so Alerts
        # with level 1 and higher, resp. for critical the level
        # was above set to 5 so 6 and higher out of worker's 0 .. 9
        # (10 Alerts altogether) shall be present
        softSink = FileSink(config.soft.sinks.file)
        criticalSink = FileSink(config.critical.sinks.file)
        softList = softSink.load()
        criticalList = criticalSink.load()
        # check soft level alerts
        # levels 1 .. 4 went in (level 0 is, according to the config not considered)
        self.assertEqual(len(softList), 3)
        for a, level in zip(softList, range(1, 4)):
            self.assertEqual(a["Level"], level)
        # check 'critical' levels
        # only levels 5 .. 9 went in
        self.assertEqual(len(criticalList), 5)
        for a, level in zip(criticalList, range(5, 10)):
            self.assertEqual(a["Level"], level)


    def testProcessorWithReceiverAndCouchSink(self):
        # set up couch first
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        dbName = "couch_sink"
        self.testInit.setupCouch(dbName)

        # add corresponding part of the configuration for CouchSink(s)
        config = self.config.AlertProcessor
        config.critical.sinks.section_("couch")
        config.critical.sinks.couch.url = self.testInit.couchUrl
        config.critical.sinks.couch.database = self.testInit.couchDbName

        # just send the Alert into couch
        processor = Processor(config)
        # Receiver is waited for shutdown / shutdown explicitly in tearDown()
        self.receiver = Receiver(self.addr, processor, self.ctrl)
        self.receiver.startReceiver() # non blocking call



        # run worker(), this time directly without Process as above,
        # worker will send 10 Alerts to Receiver
        worker(self.addr, self.ctrl, 10)

        # wait until Receiver is shut down (by a message from worker()
        # also need to wait, otherwise tearDown kicks off and scrapes the
        # couch so half of the alerts will be undelivered

        while self.receiver.isReady():
            time.sleep(ReceiverLogic.TIMEOUT_AFTER_SHUTDOWN * 1.5)
            logging.info("%s: Waiting for Receiver shutdown ..." % inspect.stack()[0][3])


if __name__ == "__main__":
    unittest.main()
