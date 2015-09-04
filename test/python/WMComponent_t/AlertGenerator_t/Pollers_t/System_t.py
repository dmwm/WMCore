"""
Tests for system-related monitoring pollers such as overall
CPU and memory utilisation, available disk space, CPU/mem
utilisation by particular processes, etc.

"""

import os
import unittest
import logging
import random
import datetime
import time
import subprocess
import signal

from WMComponent.AlertGenerator.Pollers.Base import ProcessDetail
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
        self.testInit.setLogging(logLevel = logging.DEBUG)
        self.testDir = self.testInit.generateWorkDir()
        self.config = getConfig(self.testDir)
        # mock generator instance to communicate some configuration values
        self.generator = utils.AlertGeneratorMock(self.config)


    def tearDown(self):
        self.testInit.delWorkDir()
        self.generator = None


    def testProcessCPUPollerBasic(self):
        pid = os.getpid()
        name = "mytestprocess"
        pd = ProcessDetail(pid, name)
        poller = ProcessCPUPoller()
        v = poller.sample(pd)
        self.assertTrue(isinstance(v, float))


    def testProcessMemoryPollerBasic(self):
        pid = os.getpid()
        name = "mytestprocess"
        pd = ProcessDetail(pid, name)
        poller = ProcessMemoryPoller()
        v = poller.sample(pd)
        self.assertTrue(isinstance(v, float))


    def _doPeriodPoller(self, thresholdToTest, level, config,
                        pollerClass, expected = 0):
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = pollerClass
        ti.config = config
        ti.thresholdToTest = thresholdToTest
        ti.level = level
        ti.expected = expected
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


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


    def _getKilledProcessDetail(self):
        """
        Create a process to have a valid pid, then kill it.
        Prepared in the ProcessDetail instance.

        """
        command = "sleep 300"
        proc = subprocess.Popen(command.split())
        name = "mytestkilledprocess"
        pd = ProcessDetail(proc.pid, name)
        os.kill(proc.pid, signal.SIGKILL)
        proc.poll() # necessary, otherwise it'll never end/return
        while proc.poll() == None:
            time.sleep(0.2)
            print "waiting"
        return pd


    def testProcessCPUPollerNoSuchProcess(self):
        """
        Poller should handle if the watched processed crashed or was
        terminated, so polling on NoSuchProcess.

        """
        pd = self._getKilledProcessDetail()
        poller = ProcessCPUPoller()
        self.assertFalse(pd.proc.is_running())
        # sample() shall result into handled psutil.error.NoSuchProcess
        self.assertRaises(Exception, poller.sample, pd)


    def testProcessMemoryPollerNoSuchProcess(self):
        """
        Poller should handle if the watched processed crashed or was
        terminated, so polling on NoSuchProcess.

        """
        pd = self._getKilledProcessDetail()
        poller = ProcessMemoryPoller()
        self.assertFalse(pd.proc.is_running())
        # sample() shall result into handled psutil.error.NoSuchProcess
        self.assertRaises(Exception, poller.sample, pd)


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
        poller.sample = lambda: self._dfCommandOutputGenerator(thresholdToTest,
                                                               thresholdToTest + 10)
        handler, receiver = utils.setUpReceiver(self.generator.config.Alert.address,
                                                self.generator.config.Alert.controlAddr)
        poller.start()
        self.assertTrue(poller.is_alive())

        # wait to poller to work now ... wait for alert to arrive
        if expected != 0:
            # #2238 AlertGenerator test can take 1 hour+ (and fail)
            # fail 2mins anyway if alert is not received
            timeLimitExceeded = False
            startTime = datetime.datetime.now()
            limitTime = 2 * 60 # seconds
            while len(handler.queue) == 0:
                time.sleep(config.pollInterval / 5)
                if (datetime.datetime.now() - startTime).seconds > limitTime:
                    timeLimitExceeded = True
                    break
        else:
            time.sleep(config.pollInterval * 2)

        poller.terminate()
        receiver.shutdown()
        self.assertFalse(poller.is_alive())

        if expected != 0:
            # #2238 AlertGenerator test can take 1 hour+ (and fail)
            # temporary measure from above loop:
            if timeLimitExceeded:
                self.fail("No alert received in %s seconds." % limitTime)
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
        directory = "/dev"
        poller.sample(directory)
        # check will need this attribute set
        poller._dbDirectory = directory
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
