"""
Tests for system-related monitoring pollers such as overall
CPU and memory utilisation, available disk space, CPU/mem
utilisation by particular processes, etc.

"""

import os
import unittest
import logging
import types
import multiprocessing
import random
import time
from functools import partial

from WMCore.Alerts.Alert import Alert
from WMComponent.AlertGenerator.Pollers.Base import ProcessDetail
from WMComponent.AlertGenerator.Pollers.Base import Measurements
from WMComponent.AlertGenerator.Pollers.System import ProcessCPUPoller
from WMComponent.AlertGenerator.Pollers.System import ProcessMemoryPoller
from WMComponent.AlertGenerator.Pollers.System import CPUPoller
from WMComponent.AlertGenerator.Pollers.System import MemoryPoller
from WMComponent.AlertGenerator.Pollers.System import DiskSpacePoller
from WMComponent.AlertGenerator.Pollers.System import DirectorySizePoller
from WMQuality.TestInit import TestInit
from WMComponent_t.AlertGenerator_t.AlertGenerator_t import getConfig
from WMComponent_t.AlertGenerator_t.Pollers_t import utils



class SystemTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        # still no effect, .debug, .info not appearing ...
        self.testInit.setLogging(logLevel = logging.NOTSET)
        self.testDir = self.testInit.generateWorkDir()
        self.config = getConfig(self.testDir)
        # mock generator instance to communicate some configuration values
        self.generator = utils.AlertGeneratorMock(self.config)        
        self.testProcesses = []
         
        
    def tearDown(self):       
        self.testInit.delWorkDir()
        self.generator = None
        utils.terminateProcesses(self.testProcesses)
        
            
    def testProcessCPUPollerBasic(self):
        p = utils.getProcess()
        self.testProcesses.append(p)        
        name = "mytestprocess"
        pd = ProcessDetail(p.pid, name)        
        poller = ProcessCPUPoller()
        v = poller.sample(pd)
        self.assertTrue(isinstance(v, types.FloatType))
        # psutil.error.AccessDenied will result into -1 returned
        self.assertTrue(v > 0)        
        
            
    def testProcessMemoryPollerBasic(self):
        p = utils.getProcess()
        self.testProcesses.append(p)
        name = "mytestprocess"
        pd = ProcessDetail(p.pid, name)        
        poller = ProcessMemoryPoller()
        v = poller.sample(pd)
        self.assertTrue(isinstance(v, types.FloatType))
        # psutil.error.AccessDenied will result into -1 returned
        self.assertTrue(v > 0)
        

    def _doPeriodPoller(self, thresholdToTest, level, config,
                        pollerClass, expected = 0):
        handler, receiver = utils.setUpReceiver(self.generator.config.Alert.address,
                                                self.generator.config.Alert.controlAddr)    
        numMeasurements = config.period / config.pollInterval
        poller = pollerClass(config, self.generator)
        # inject own input sample data provider, there will be 1 input argument we don't want here
        poller.sample = lambda _: random.randint(thresholdToTest, thresholdToTest + 10)
        proc = multiprocessing.Process(target = poller.poll, args = ())
        proc.start()
        self.assertTrue(proc.is_alive())

        if expected != 0:
            # wait to poller to work now ... wait for alert to arrive
            while len(handler.queue) == 0:
                time.sleep(config.pollInterval)
        else:
            # no alert shall arrive
            time.sleep(config.period * 2)
            
        proc.terminate()
        poller.shutdown()
        receiver.shutdown()
        self.assertFalse(proc.is_alive())
        
        if expected != 0:
            # there should be just one alert received, poller should have the
            # change to send a second
            self.assertEqual(len(handler.queue), expected)
            a = handler.queue[0]
            # soft threshold - alert should have soft level
            self.assertEqual(a["Level"], level)
            self.assertEqual(a["Component"], self.generator.__class__.__name__)
            self.assertEqual(a["Source"], poller.__class__.__name__)
            self.assertEqual(a["Details"]["numMeasurements"], numMeasurements)
        else:
            self.assertEqual(len(handler.queue), expected)
        
    
    def testCPUPollerSoftThreshold(self):
        self.config.AlertGenerator.cpuPoller.soft = 70
        self.config.AlertGenerator.cpuPoller.critical = 80
        self.config.AlertGenerator.cpuPoller.pollInterval = 0.2
        self.config.AlertGenerator.cpuPoller.period = 1
        level = self.config.AlertProcessor.soft.level
        thresholdToTest = self.config.AlertGenerator.cpuPoller.soft
        self._doPeriodPoller(thresholdToTest, level, self.config.AlertGenerator.cpuPoller,
                             CPUPoller, expected = 1)
        
        
    def testCPUPollerCriticalThreshold(self):
        self.config.AlertGenerator.cpuPoller.soft = 70
        self.config.AlertGenerator.cpuPoller.critical = 80
        self.config.AlertGenerator.cpuPoller.pollInterval = 0.2
        self.config.AlertGenerator.cpuPoller.period = 1
        level = self.config.AlertProcessor.critical.level
        thresholdToTest = self.config.AlertGenerator.cpuPoller.critical
        self._doPeriodPoller(thresholdToTest, level, self.config.AlertGenerator.cpuPoller,
                             CPUPoller, expected = 1)
       
       
    def testCPUPollerNoAlert(self):
        """
        This test actually does more than one measurement.
        
        """
        self.config.AlertGenerator.cpuPoller.soft = 70
        self.config.AlertGenerator.cpuPoller.critical = 80
        self.config.AlertGenerator.cpuPoller.pollInterval = 0.2
        self.config.AlertGenerator.cpuPoller.period = 1
        level = 0
        thresholdToTest = self.config.AlertGenerator.cpuPoller.soft - 20
        self._doPeriodPoller(thresholdToTest, level, self.config.AlertGenerator.cpuPoller,
                             CPUPoller, expected = 0)
        
    
    def testMemoryPollerBasic(self):
        self.config.AlertGenerator.memPoller.soft = 70
        self.config.AlertGenerator.memPoller.critical = 80
        self.config.AlertGenerator.memPoller.pollInterval = 0.2
        self.config.AlertGenerator.memPoller.period = 1
        poller = MemoryPoller(self.config.AlertGenerator.memPoller, self.generator)
        poller.check()
        
                
    def testMemoryPollerSoftThreshold(self):
        self.config.AlertGenerator.memPoller.soft = 70
        self.config.AlertGenerator.memPoller.critical = 80
        self.config.AlertGenerator.memPoller.pollInterval = 0.2
        self.config.AlertGenerator.memPoller.period = 1
        level = self.config.AlertProcessor.soft.level
        thresholdToTest = self.config.AlertGenerator.memPoller.soft
        self._doPeriodPoller(thresholdToTest, level, self.config.AlertGenerator.memPoller,
                             MemoryPoller, expected = 1)


    def testMemoryPollerCriticalThreshold(self):
        self.config.AlertGenerator.memPoller.soft = 70
        self.config.AlertGenerator.memPoller.critical = 80
        self.config.AlertGenerator.memPoller.pollInterval = 0.2
        self.config.AlertGenerator.memPoller.period = 1
        level = self.config.AlertProcessor.critical.level
        thresholdToTest = self.config.AlertGenerator.memPoller.critical
        self._doPeriodPoller(thresholdToTest, level, self.config.AlertGenerator.memPoller,
                             MemoryPoller, expected = 1)


    def testMemoryPollerNoAlert(self):
        """
        This test actually does more than one measurement.
        
        """
        self.config.AlertGenerator.memPoller.soft = 70
        self.config.AlertGenerator.memPoller.critical = 80
        self.config.AlertGenerator.memPoller.pollInterval = 0.2
        self.config.AlertGenerator.memPoller.period = 1
        level = 0
        thresholdToTest = self.config.AlertGenerator.memPoller.soft - 20
        self._doPeriodPoller(thresholdToTest, level, self.config.AlertGenerator.memPoller,
                             MemoryPoller, expected = 0)
        

    def testDiskSpacePollerBasic(self):
        self.config.AlertGenerator.diskSpacePoller.soft = 60
        self.config.AlertGenerator.diskSpacePoller.critical = 90
        self.config.AlertGenerator.diskSpacePoller.pollInterval = 0.2
        poller = DiskSpacePoller(self.config.AlertGenerator.diskSpacePoller, self.generator)
        poller.sample()
        # this may send an alert, provide sender
        poller.sender = utils.SenderMock()
        poller.check()
        

    @staticmethod
    def _dfCommandOutputGenerator(low, high):
        # percentage signs make it difficult to string interpolate
        out1 = """Filesystem           1K-blocks      Used Available Use% Mounted on
/dev/sda2              1953276    382040   1467026  """
        out3 = """%  /data
udev                   4085528       336   4085192   1% /dev
none                   4085528       628   4084900   1% /dev/shm
        
""" 
        return out1 + str(random.randint(low, high)) + out3
    
    
    def _doDiskPoller(self, thresholdToTest, level, config, expected = 0):
        poller = DiskSpacePoller(config, self.generator)
        # inject own input sample data provider
        poller.sample = partial(self._dfCommandOutputGenerator, thresholdToTest, thresholdToTest + 10)
        
        handler, receiver = utils.setUpReceiver(self.generator.config.Alert.address,
                                                self.generator.config.Alert.controlAddr)    
        proc = multiprocessing.Process(target = poller.poll, args = ())
        proc.start()
        self.assertTrue(proc.is_alive())

        # wait to poller to work now ... wait for alert to arrive
        if expected != 0:
            while len(handler.queue) == 0:
                time.sleep(config.pollInterval / 5)
        else:
            time.sleep(config.pollInterval * 2)

        proc.terminate()
        poller.shutdown()
        receiver.shutdown()
        self.assertFalse(proc.is_alive())
        
        if expected != 0:
            # there should be just one alert received, poller should have the
            # change to send a second
            self.assertEqual(len(handler.queue), expected)
            a = handler.queue[0]
            # soft threshold - alert should have soft level
            self.assertEqual(a["Level"], level)
            self.assertEqual(a["Component"], self.generator.__class__.__name__)
            self.assertEqual(a["Source"], poller.__class__.__name__)
            d = a["Details"]
            self.assertEqual(d["mountPoint"], "/data")
            self.assertEqual(d["threshold"], "%s%%" % thresholdToTest)
        else:
            self.assertEqual(len(handler.queue), 0)
        
        
    def testDiskSpacePollerSoftThreshold(self):
        self.config.AlertGenerator.diskSpacePoller.soft = 60
        self.config.AlertGenerator.diskSpacePoller.critical = 90
        self.config.AlertGenerator.diskSpacePoller.pollInterval = 0.2
        level = self.config.AlertProcessor.soft.level
        thresholdToTest = self.config.AlertGenerator.diskSpacePoller.soft
        self._doDiskPoller(thresholdToTest, level, self.config.AlertGenerator.diskSpacePoller,
                           expected = 1)
     
        
    def testDiskSpacePollerCriticalThreshold(self):
        self.config.AlertGenerator.diskSpacePoller.soft = 60
        self.config.AlertGenerator.diskSpacePoller.critical = 90
        self.config.AlertGenerator.diskSpacePoller.pollInterval = 0.2
        level = self.config.AlertProcessor.critical.level
        thresholdToTest = self.config.AlertGenerator.diskSpacePoller.critical
        self._doDiskPoller(thresholdToTest, level, self.config.AlertGenerator.diskSpacePoller,
                           expected = 1)
    
    
    def testDiskSpacePollerNoAlert(self):
        """
        This test actually does more than one measurement.
        
        """
        self.config.AlertGenerator.diskSpacePoller.soft = 70
        self.config.AlertGenerator.diskSpacePoller.critical = 80
        self.config.AlertGenerator.diskSpacePoller.pollInterval = 0.2
        level = 0
        thresholdToTest = self.config.AlertGenerator.diskSpacePoller.soft - 20
        self._doDiskPoller(thresholdToTest, level, self.config.AlertGenerator.diskSpacePoller,
                           expected = 0)
        
        
    def testDirectorySizePollerBasic(self):
        self.config.AlertGenerator.section_("bogusSizePoller")
        self.config.AlertGenerator.bogusSizePoller.soft = 5
        self.config.AlertGenerator.bogusSizePoller.critical = 10
        self.config.AlertGenerator.bogusSizePoller.pollInterval = 0.2
        poller = DirectorySizePoller(self.config.AlertGenerator.bogusSizePoller, self.generator)
        dir = "/dev"
        poller.sample(dir)
        # check will need this attribute set
        poller._dbDirectory = dir
        poller.check()
        
        
    def testDirectorySizePollerUnitTest(self):
        config = getConfig("/tmp")
        generator = utils.AlertGeneratorMock(config)
        poller = DirectorySizePoller(config.AlertGenerator.mysqlDbSizePoller, generator,
                                     unitSelection = 1) # kilobytes
        poller.sender = lambda alert: 1 + 1
        self.assertEqual(poller._currSizeUnit, "kB")
        self.assertEqual(poller._prefixBytesFactor, 1024)
        # this actually tests the real sample method
        poller._dbDirectory = "/dev"
        poller.check() # calls sample() automatically
        
        

if __name__ == "__main__":
    unittest.main()