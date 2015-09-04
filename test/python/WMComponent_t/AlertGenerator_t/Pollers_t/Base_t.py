"""
Tests for Pollers/Base pollers and other base/auxiliary classes.

The warnings below apply to all tests featuring Sender, Receiver and
real sending of Alert instances.

    caveat 1:

    Receiver instance to be kept in a local variable.
    Making it instance variable leads to "Address already in use."
    Causes not properly investigated.

    TODO: understand the above problem, the code below fails:

    def testSenderReceiver1(self):
        self.sender, self.handler, self.receiver = setUpReceiver()
        self.receiver.shutdown()

    def testSenderReceiver2(self):
        self.sender, self.handler, self.receiver = setUpReceiver()
        self.receiver.shutdown()


    caveat 2:

    Sender instance needs to be created in the process which performs the actual
    alert send. Passing sender instance to the process leads to data being sent
    but never received and test remains hanging:
    Exception zmq.core.error.ZMQError:  in <zmq.core.context.Context object at 0x2b76a50>
    In short, each alert sending entity to own its Sender instance, not to be
    passed it into.

    This was true when Pollers were implemented by means of multiprocessing.Process
    in the chain of tickets referenced from #2258, pollers are now Threads.

"""

import os
import unittest
import logging
import time
import shutil
import inspect

import psutil
from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sender import Sender
from WMQuality.TestInit import TestInit
from WMComponent_t.AlertGenerator_t.AlertGenerator_t import getConfig
from WMComponent_t.AlertGenerator_t.Pollers_t import utils
from WMComponent.AlertGenerator.Pollers.Base import ProcessDetail
from WMComponent.AlertGenerator.Pollers.Base import Measurements
from WMComponent.AlertGenerator.Pollers.Base import BasePoller
from WMComponent.AlertGenerator.Pollers.Base import PeriodPoller
from WMComponent.AlertGenerator.Pollers.Agent import ComponentsCPUPoller


class BaseTest(unittest.TestCase):
    """
    Some methods of this class are made static and are used from
    other test cases.

    """
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging(logLevel = logging.DEBUG)
        self.testDir = self.testInit.generateWorkDir()
        self.config = getConfig(self.testDir)
        self.testComponentDaemonXml = "/tmp/TestComponent/Daemon.xml"


    def tearDown(self):
        self.testInit.delWorkDir()
        self.generator = None
        # if the directory and file "/tmp/TestComponent/Daemon.xml" after
        # ComponentsPoller test exist, then delete it
        d = os.path.dirname(self.testComponentDaemonXml)
        if os.path.exists(d):
            shutil.rmtree(d)


    def testSenderReceiverBasic(self):
        sender = Sender(self.config.Alert.address,
                        self.config.Alert.controlAddr,
                        self.__class__.__name__)
        handler, receiver = utils.setUpReceiver(self.config.Alert.address,
                                                self.config.Alert.controlAddr)
        a = Alert(Component = inspect.stack()[0][3])
        sender(a)
        while len(handler.queue) == 0:
            time.sleep(0.5)
            print "%s waiting for alert to arrive" % inspect.stack()[0][3]
        receiver.shutdown()
        self.assertEqual(len(handler.queue), 1)
        self.assertEqual(handler.queue[0]["Component"], inspect.stack()[0][3])

    def testProcessDetailBasic(self):
        pid = os.getpid()
        name = inspect.stack()[0][3]  # test name
        pd = ProcessDetail(pid, name)
        self.assertEqual(pd.pid, pid)
        self.assertEqual(pd.name, name)
        self.assertEqual(pd.proc.pid, pid)
        numChildren = None
        try:
            numChildren = len(psutil.Process(pid).children())  # psutil 3.1.1
        except AttributeError:
            numChildren = len(psutil.Process(pid).get_children())  # psutil 0.6.1

        self.assertEqual(len(pd.children), numChildren)
        self.assertEqual(len(pd.allProcs), 1 + numChildren)
        d = pd.getDetails()
        self.assertEqual(d["pid"], pid)
        self.assertEqual(d["component"], name)
        self.assertEqual(d["numChildrenProcesses"], numChildren)
        pd.refresh()

    def testMeasurementsBasic(self):
        numMes = 10
        mes = Measurements(numMes)
        self.assertEqual(mes._numOfMeasurements, numMes)
        self.assertEqual(len(mes), 0)
        mes.append(20)
        self.assertEqual(len(mes), 1)
        self.assertEqual(mes[0], 20)
        mes.append(30)
        self.assertEqual(mes[1], 30)
        mes.clear()
        self.assertEqual(len(mes), 0)
        self.assertEqual(mes._numOfMeasurements, numMes)


    def testBasePollerBasic(self):
        config = getConfig("/tmp")
        # create some non-sence config section. just need a bunch of values defined
        config.AlertGenerator.section_("bogusPoller")
        config.AlertGenerator.bogusPoller.soft = 5 # [percent]
        config.AlertGenerator.bogusPoller.critical = 50 # [percent]
        config.AlertGenerator.bogusPoller.pollInterval = 2  # [second]
        config.AlertGenerator.bogusPoller.period = 10

        generator = utils.AlertGeneratorMock(config)
        poller = BasePoller(config.AlertGenerator.bogusPoller, generator)
        # define dummy check method
        poller.check = lambda: 1+1
        poller.start()
        # poller now runs
        time.sleep(config.AlertGenerator.bogusPoller.pollInterval * 2)
        poller.terminate()
        while poller.is_alive():
            time.sleep(0.2)
            print "%s waiting for test poller to terminate" % inspect.stack()[0][3]


    def testBasePollerHandleFailedPolling(self):
        config = getConfig("/tmp")
        # create some non-sence config section. just need a bunch of values defined
        config.AlertGenerator.section_("bogusPoller")
        config.AlertGenerator.bogusPoller.soft = 5 # [percent]
        config.AlertGenerator.bogusPoller.critical = 50 # [percent]
        config.AlertGenerator.bogusPoller.pollInterval = 2  # [second]
        config.AlertGenerator.bogusPoller.period = 10

        generator = utils.AlertGeneratorMock(config)
        poller = BasePoller(config.AlertGenerator.bogusPoller, generator)
        ex = Exception("test exception")
        class Sender(object):
            def __call__(self, alert):
                self.alert = alert
        poller.sender = Sender()
        poller._handleFailedPolling(ex)
        self.assertEqual(poller.sender.alert["Source"], "BasePoller")


    def testPeriodPollerOnRealProcess(self):
        config = getConfig("/tmp")
        config.component_("AlertProcessor")
        config.AlertProcessor.section_("critical")
        config.AlertProcessor.section_("soft")
        config.AlertProcessor.critical.level = 5
        config.AlertProcessor.soft.level = 0
        config.component_("AlertGenerator")
        config.AlertGenerator.section_("bogusPoller")
        config.AlertGenerator.bogusPoller.soft = 5 # [percent]
        config.AlertGenerator.bogusPoller.critical = 50 # [percent]
        config.AlertGenerator.bogusPoller.pollInterval = 0.2  # [second]
        # period during which measurements are collected before evaluating for
        # possible alert triggering
        config.AlertGenerator.bogusPoller.period = 1

        generator = utils.AlertGeneratorMock(config)
        poller = PeriodPoller(config.AlertGenerator.bogusPoller, generator)
        poller.sender = utils.SenderMock()
        # get CPU usage percentage, it's like measuring CPU usage of a real
        # component, so use the appropriate poller's method for that
        # (PeriodPoller itself is higher-level class so it doesn't define
        # a method to provide sampling data)
        poller.sample = lambda processDetail: ComponentsCPUPoller.sample(processDetail)

        # get own pid
        pid = os.getpid()
        name = inspect.stack()[0][3] # test name
        pd = ProcessDetail(pid, name)
        # need to repeat sampling required number of measurements
        numOfMeasurements = int(config.AlertGenerator.bogusPoller.period /
                                config.AlertGenerator.bogusPoller.pollInterval)
        mes = Measurements(numOfMeasurements)
        self.assertEqual(len(mes), 0)
        for i in range(mes._numOfMeasurements):
            poller.check(pd, mes)

        # since the whole measurement cycle was done, values should have been nulled
        self.assertEqual(len(mes), 0)


    def testPeriodPollerCalculationPredefinedInput(self):
        config = getConfig("/tmp")
        config.component_("AlertProcessor")
        config.AlertProcessor.section_("critical")
        config.AlertProcessor.section_("soft")
        config.AlertProcessor.critical.level = 5
        config.AlertProcessor.soft.level = 0
        config.component_("AlertGenerator")
        config.AlertGenerator.section_("bogusPoller")
        # put some threshold numbers, just need to check output calculation
        # from check() method
        config.AlertGenerator.bogusPoller.soft = 5 # [percent]
        config.AlertGenerator.bogusPoller.critical = 50 # [percent]
        config.AlertGenerator.bogusPoller.pollInterval = 0.2  # [second]
        config.AlertGenerator.bogusPoller.period = 1

        generator = utils.AlertGeneratorMock(config)
        poller = PeriodPoller(config.AlertGenerator.bogusPoller, generator)
        # since poller may trigger an alert, give it mock sender
        poller.sender = utils.SenderMock()
        # provide sample method with predefined input, float
        predefInput = 10.12
        poller.sample = lambda processDetail: predefInput

        processDetail = None
        numOfMeasurements = int(config.AlertGenerator.bogusPoller.period /
                                config.AlertGenerator.bogusPoller.pollInterval)
        mes = Measurements(numOfMeasurements)
        for i in range(mes._numOfMeasurements):
            poller.check(processDetail, mes)

        # the above loop should went 5 times, should reach evaluation of 5 x predefInput
        # values, the average should end up 10, which should trigger soft threshold
        self.assertEqual(len(poller.sender.queue), 1)
        a = poller.sender.queue[0]

        self.assertEqual(a["Component"], generator.__class__.__name__)
        self.assertEqual(a["Source"], poller.__class__.__name__)
        d = a["Details"]
        self.assertEqual(d["threshold"], "%s%%" % config.AlertGenerator.bogusPoller.soft)
        self.assertEqual(d["numMeasurements"], mes._numOfMeasurements)
        self.assertEqual(d["period"], config.AlertGenerator.bogusPoller.period)
        self.assertEqual(d["average"], "%s%%" % predefInput)
        # since the whole measurement cycle was done, values should have been nulled
        self.assertEqual(len(mes), 0)
        # poller wasn't really started so no need to terminate it



if __name__ == "__main__":
    unittest.main()
