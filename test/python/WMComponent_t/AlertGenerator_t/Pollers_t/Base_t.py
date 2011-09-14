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
import random
import shutil
import types

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
from WMComponent.AlertGenerator.Pollers.Agent import ComponentsPoller
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
        self.testProcesses = []
        self.testComponentDaemonXml = "/tmp/TestComponent/Daemon.xml" 
        
        
    def tearDown(self):       
        self.testInit.delWorkDir()
        self.generator = None
        utils.terminateProcesses(self.testProcesses)
        
        # if the directory and file "/tmp/TestComponent/Daemon.xml" after
        # ComponentsPoller test exist, then delete it
        d = os.path.dirname(self.testComponentDaemonXml)
        if os.path.exists(d):
            shutil.rmtree(d)
                    

    def testSenderReceiverBasic(self):
        sender = Sender(self.config.Alert.address,
                        self.__class__.__name__,
                        self.config.Alert.controlAddr)
        handler, receiver = utils.setUpReceiver(self.config.Alert.address,
                                                self.config.Alert.controlAddr)
        a = Alert(Component = "testSenderReceiverBasic")
        sender(a)
        time.sleep(0.5)
        self.assertEqual(len(handler.queue), 1)
        self.assertEqual(handler.queue[0]["Component"], "testSenderReceiverBasic")
        receiver.shutdown()
        
            
    def testProcessDetailBasic(self):
        p = utils.getProcess()
        self.testProcesses.append(p)
        name = "mytestprocess"
        pd = ProcessDetail(p.pid, name)
        self.assertEqual(pd.pid, p.pid)
        self.assertEqual(pd.name, name)
        self.assertEqual(pd.proc.pid, p.pid)
        self.assertEqual(len(pd.children), 0)
        self.assertEqual(len(pd.allProcs), 1)
        utils.terminateProcesses(self.testProcesses)
        d = pd.getDetails()
        self.assertEqual(d["pid"], p.pid)
        self.assertEqual(d["component"], name)
        self.assertEqual(d["numChildrenProcesses"], 0)
        
        
    def testProcessDetailChildren(self):
        numSubProcesses = 3
        p = utils.getProcess(numChildren = numSubProcesses)
        self.testProcesses.append(p)
        # wait until all desired processes are running
        while len(psutil.Process(p.pid).get_children()) < numSubProcesses:
            print "waiting for children processes to start"
            time.sleep(0.5)        
        name = "mytestprocess2"
        pd = ProcessDetail(p.pid, name)
        self.assertEqual(pd.proc.pid, p.pid)
        self.assertEqual(len(pd.children), numSubProcesses)
        self.assertEqual(len(pd.allProcs), numSubProcesses + 1)
        utils.terminateProcesses(self.testProcesses)
        d = pd.getDetails()
        self.assertEqual(d["pid"], p.pid)
        self.assertEqual(d["numChildrenProcesses"], numSubProcesses)
        
        
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
        time.sleep(0.1)
        poller.terminate()
            
        
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
        # the way the worker is implemented should take 100% CPU, but it sometimes
        # take a while, test safer threshold here, testing thresholds
        # more rigorously happens in Pollers_t
        config.AlertGenerator.bogusPoller.critical = 50 # [percent] 
        config.AlertGenerator.bogusPoller.pollInterval = 0.2  # [second]
        # period during which measurements are collected before evaluating for possible alert triggering
        config.AlertGenerator.bogusPoller.period = 1
        
        generator = utils.AlertGeneratorMock(config)
        poller = PeriodPoller(config.AlertGenerator.bogusPoller, generator)
        poller.sender = utils.SenderMock()
        # get CPU usage percentage, it's like measuring CPU usage of a real
        # component, so use the appropriate poller's method for that
        # (PeriodPoller itself is higher-level class so it doesn't define
        # a method to provide sampling data)
        poller.sample = lambda processDetail: ComponentsCPUPoller.sample(processDetail)
        
        p = utils.getProcess()
        self.testProcesses.append(p)
        while not p.is_alive():
            time.sleep(0.2)        
        name = "mytestprocess-testPeriodPollerBasic"
        pd = ProcessDetail(p.pid, name)
        # need to repeat sampling required number of measurements
        numOfMeasurements = int(config.AlertGenerator.bogusPoller.period / 
                                config.AlertGenerator.bogusPoller.pollInterval)
        mes = Measurements(numOfMeasurements)
        self.assertEqual(len(mes), 0)
        for i in range(mes._numOfMeasurements):
            poller.check(pd, mes)
            
        # 1 alert should have arrived, test it
        #    though there may be a second alert as well if the test managed to
        #    run second round - don't test number of received alerts
        #    also the Level and threshold is not deterministic: given it's
        #    measured on a live process it can't be determined up-front how
        #    much CPU this simple process will be given: don't test Level
        #    and threshold
        a = poller.sender.queue[0]
        self.assertEqual(a["Component"], generator.__class__.__name__)
        self.assertEqual(a["Source"], poller.__class__.__name__)
        d = a["Details"]
        self.assertEqual(d["numMeasurements"], mes._numOfMeasurements)
        self.assertEqual(d["component"], name)
        self.assertEqual(d["period"], config.AlertGenerator.bogusPoller.period)
        
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
        
        

if __name__ == "__main__":
    unittest.main()        