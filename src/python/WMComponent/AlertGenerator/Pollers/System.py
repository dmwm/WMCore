"""
Common module for system-related monitoring such as overall
CPU and memory utilisation, available disk space, CPU/mem
utilisation by particular processes, etc.

"""

import logging
import subprocess

import psutil

from WMCore.Alerts.Alert import Alert
from WMComponent.AlertGenerator.Pollers.Base import Measurements
from WMComponent.AlertGenerator.Pollers.Base import BasePoller
from WMComponent.AlertGenerator.Pollers.Base import PeriodPoller



class ProcessCPUPoller(object):
    """
    Common class for polling CPU usage of a process.

    """

    @staticmethod
    def sample(processDetail):
        """
        ProcessDetail input may constitute from the main process and subprocesses:
        iterate over all and accumulate a summary.
        Method psutil.Process.get_cpu_percent provides process information.

        """
        try:
            # raises: psutil.error.AccessDenied, psutil.error.NoSuchProcess
            pollProcess = lambda proc: proc.get_cpu_percent(PeriodPoller.PSUTIL_INTERVAL)
            v = sum([pollProcess(p) for p in processDetail.allProcs])
            return v
        except psutil.error.AccessDenied as ex:
            m = ("Can't get CPU usage of %s, reason: %s" %
                 (processDetail.getDetails(), ex))
            raise Exception(m)
        # psutil.error.NoSuchProcess is handled higher



class ProcessMemoryPoller(object):
    """
    Common class for polling memory usage of a process.

    """

    @staticmethod
    def sample(processDetail):
        """
        ProcessDetail input may constitute from the main process and subprocesses:
        iterate over all and accumulate a summary.
        method of psutil.Process is used: compares physical system memory to
        process resident memory and calculate process memory utilization as a
        percentage. Here also incl. subprocesses.

        """
        try:
            # get_memory_info(): returns RSS, VMS tuple (for reference)
            # raises: psutil.error.AccessDenied, psutil.error.NoSuchProcess
            pollProcess = lambda proc: proc.get_memory_percent()
            v = sum([pollProcess(p) for p in processDetail.allProcs])
            return v
        except psutil.error.AccessDenied as ex:
            m = ("Can't get memory usage of %s, reason: %s" %
                 (processDetail.getDetails(), ex))
            raise Exception(m)
        # psutil.error.NoSuchProcess is handled higher



class CPUPoller(PeriodPoller):
    """
    Class to handle polling of overall system CPU load.

    """
    def __init__(self, config, generator):
        PeriodPoller.__init__(self, config, generator)
        numOfMeasurements = round(self.config.period / self.config.pollInterval, 0)
        self._measurements = Measurements(numOfMeasurements)


    @staticmethod
    def sample(_):
        """
        Overall system's CPU load in percentage.
        Unused input satisfies general sample(ProcessDetail) API for which
        None is passed here (see below).

        """
        return psutil.cpu_percent(PeriodPoller.PSUTIL_INTERVAL)


    def check(self):
        PeriodPoller.check(self, None, self._measurements)



class MemoryPoller(PeriodPoller):
    """
    Class to handle overall system memory utilisation.

    """
    def __init__(self, config, generator):
        PeriodPoller.__init__(self, config, generator)
        numOfMeasurements = round(self.config.period / self.config.pollInterval, 0)
        self._measurements = Measurements(numOfMeasurements)


    @staticmethod
    def sample(_):
        """
        Returns system physical memory utilisation percentage.
        Unused input satisfies general sample(ProcessDetail) API for which
        None is passed here (see check below).

        """
        # psutil.phymem_usage(): returns:
        # usage(total=8367161344, used=3072045056, free=5295116288, percent=26.800000000000001)
        usedPhyMemPercent = psutil.phymem_usage().percent
        return usedPhyMemPercent


    def check(self):
        PeriodPoller.check(self, None, self._measurements)



class DiskSpacePoller(BasePoller):
    """
    DiskSpacePoller checks all partitions listed by
    df (report file system disk space usage)
    and if any/each partition exceeds the threshold, alert is sent.

    newer version of psutil will have (disk space poller):
        for part in psutil.disk_partitions(0):
            usage = psutil.disk_usage(part.mountpoint)

    """

    def __init__(self, config, generator):
        BasePoller.__init__(self, config, generator)


    def sample(self):
        """
        Returns output of df program.

        """
        c = "df"
        rc, err = -1, ""
        try:
            p = subprocess.Popen(c.split(),
                                 stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            rc = p.wait()
            out, err = p.communicate()
        except OSError, ex:
            err += "exception while running command '%s', reason: %s" % (c, ex)

        if rc != 0:
            m = "%s: could not check free disk space, reason: %s" % (self.__class__.__name__, err)
            logging.error(m)
            return None
        return out


    def check(self):
        """
        Checks the output of df command for percentage of disk space usage.
        The command output pattern:
        '
        Filesystem           1K-blocks      Used Available Use% Mounted on
        /dev/sda2              1953276    382040   1467026  21% /
        udev                   4085528       336   4085192   1% /dev
        none                   4085528       628   4084900   1% /dev/shm

        '

        """
        out = self.sample()
        if out == None:
            # should be logged above
            return

        percs = []
        try:
        # don't do the first line and also the last line is empty (iterate over partitions)
            for line in out.split('\n')[1:-1]:
                arr = line.split()
                if len(arr) < 6: # 6 elements on the partition entry of df output
                    continue
                percStr, mount = arr[4:6] # see the df output pattern
                if mount == "/usr/vice/cache": # do not check AFS cache dir
                    continue
                perc = int(percStr[:-1]) # without the percent sign
                for threshold, level in zip(self.thresholds, self.levels):
                    if perc >= threshold:
                        details = dict(mountPoint = mount, usage = "%s%%" % perc,
                                       threshold = "%s%%" % threshold)
                        a = Alert(**self.preAlert)
                        a.setTimestamp()
                        a["Source"] = self.__class__.__name__
                        a["Details"] = details
                        a["Level"] = level
                        logging.debug("Sending an alert (%s): %s" % (self.__class__.__name__, a))
                        self.sender(a)
                        break # send only one alert, critical threshold tested first
                percs.append(percStr)
        except (ValueError, IndexError), ex:
            logging.error("Could not check available disk space, reason: %s" % ex)
        m = "%s: measurements results: %s" % (self.__class__.__name__, percs)
        logging.debug(m)



class DirectorySizePoller(BasePoller):
    """
    Watching size of a directory.

    """
    def __init__(self, config, generator, unitSelection = 3):
        """
        Size units selection:
        0 = Bytes, 1 = kiloBytes, , 2 = MegaBytes, 3 = GigaBytes

        """
        BasePoller.__init__(self, config, generator)
        # set size units
        self._unitSelection = unitSelection
        self._sizeUnitsAll = ("B", "kB", "MB", "GB")
        self._prefixBytesFactor = float(1024 ** self._unitSelection)
        self._currSizeUnit = self._sizeUnitsAll[self._unitSelection]
        self._myName = self.__class__.__name__


    def sample(self, dir):
        """
        Returns number of bytes multiplied by PREFIX_BYTES_FACTOR of
        a directory specified in the input.

        """
        rc, err = -1, ""
        c = "du %s" % dir
        try:
            p = subprocess.Popen(c.split(),
                                 stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            rc = p.wait()
            out, err = p.communicate()
        except OSError, ex:
            err += "exception while running command '%s', reason: %s" % (c, ex)

        if rc != 0:
            m = "%s: could not get directory space usage, reason: %s" % (self._myName, err)
            logging.error(m)
            return None

        # du command output format assumption (out variable set now):
        # separate output to lines by '\n'
        # desired result is on the second from the end, the very last shall be empty
        # and format is '^size dir' ; leave on separate lines so it's obvious when something
        # fails
        try:
            lines = out.split('\n')
            desired = lines[-2]
            size = desired.split()[0]
            size = int(size)
        except Exception, ex:
            m = ("%s: could not get directory space usage, reason: %s. du output: '%s"'' %
                 (self._myName, ex, out))
            logging.error(m)
            return None
        return round(size / self._prefixBytesFactor, 3)


    def check(self):
        """
        First gets number on directory usage.
        If the usage exceeds soft, resp. critical limits, the alert is sent.

        """
        if not self._dbDirectory:
            return
        usage = self.sample(self._dbDirectory)
        if usage == None:
            # should be logged above
            return

        usageStr = "%s %s" % (usage, self._currSizeUnit)
        for threshold, level in zip(self.thresholds, self.levels):
            if usage >= threshold:
                details = dict(databasedir = self._dbDirectory, usage = usageStr,
                               threshold = threshold)
                a = Alert(**self.preAlert)
                a.setTimestamp()
                a["Source"] = self._myName
                a["Details"] = details
                a["Level"] = level
                logging.debug("Sending an alert (%s): %s" % (self.__class__.__name__, a))
                self.sender(a)
                break # send only one alert, critical threshold tested first
        m = "%s: measurements results: %s" % (self._myName, usageStr)
        logging.debug(m)
