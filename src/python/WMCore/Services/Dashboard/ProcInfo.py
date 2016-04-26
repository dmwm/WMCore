
"""
 * ApMon - Application Monitoring Tool
 * Version: 2.2.20
 *
 * Copyright (C) 2006 California Institute of Technology
 *
 * Permission is hereby granted, free of charge, to use, copy and modify
 * this software and its documentation (the "Software") for any
 * purpose, provided that existing copyright notices are retained in
 * all copies and that this notice is included verbatim in any distributions
 * or substantial portions of the Software.
 * This software is a part of the MonALISA framework (http://monalisa.cacr.caltech.edu).
 * Users of the Software are asked to feed back problems, benefits,
 * and/or suggestions about the software to the MonALISA Development Team
 * (developers@monalisa.cern.ch). Support for this software - fixing of bugs,
 * incorporation of new features - is done on a best effort basis. All bug
 * fixes and enhancements will be made available under the same terms and
 * conditions as the original software,

 * IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES ARISING OUT
 * OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY DERIVATIVES THEREOF,
 * EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 * THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE, AND NON-INFRINGEMENT. THIS SOFTWARE IS
 * PROVIDED ON AN "AS IS" BASIS, AND THE AUTHORS AND DISTRIBUTORS HAVE NO
 * OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
 * MODIFICATIONS.
"""
from __future__ import print_function
from __future__ import division
from WMCore.Services.Dashboard.Logger import Logger
import socket
import os
import re
import time


class ProcInfo(object):
    """ ProcInfo extracts infro from the proc/ filesystem
    for system and job monitoring
    """

    # ProcInfo constructor
    def __init__(self, logger):
        self.data = {}             # monitored data that is going to be reported
        self.lastUpdateTime = 0  # when the last measurement was done
        self.jobs = {}             # jobs that will be monitored
        self.logger = logger	    # use the given logger
        self.readGenericInfo()


    def update(self):
        """
        This should be called from time to time to update the monitored data,
        but not more often than once a second because of the resolution of time()
        """
        if self.lastUpdateTime == int(time.time()):
            self.logger.log(Logger.NOTICE, "ProcInfo: update() called too often!")
            return
        self.readStat()
        self.readMemInfo()
        self.readUptimeAndLoadAvg()
        self.countProcesses()
        self.readNetworkInfo()
        self.readNetStat()
        for pid in list(self.jobs.keys()):
            self.readJobInfo(pid)
            self.readJobDiskUsage(pid)
        self.lastUpdateTime = int(time.time())
        self.data['TIME'] = int(time.time())


    def addJobToMonitor(self, pid, workDir):
        """ Call this to add another PID to be monitored """
        self.jobs[pid] = {}
        self.jobs[pid]['WORKDIR'] = workDir
        self.jobs[pid]['DATA'] = {}
        # print self.jobs


    def removeJobToMonitor(self, pid):
        """ Call this to stop monitoring a PID """
        if pid in self.jobs:
            del self.jobs[pid]


    def getSystemData(self, params, prevDataRef=None):
        """ Return a filtered hash containting the system-related parameters and values """
        return self.getFilteredData(self.data, params, prevDataRef)


    def getJobData(self, pid, params):
        """ Return a filtered hash containing the job-related parameters and values """
        if pid not in self.jobs:
            return []
        return self.getFilteredData(self.jobs[pid]['DATA'], params)

    ############################################################################################
    # internal functions for system monitoring
    ############################################################################################

    def readStat(self):
        """
        this has to be run twice (with the $lastUpdateTime updated) to get some useful results
        the information about blocks_in/out and swap_in/out isn't available for 2.6 kernels (yet)
        """
        try:
            with open('/proc/stat') as fd:
                line = fd.readline()
                while line != '':
                    if line.startswith("cpu "):
                        elem = re.split(r"\s+", line)
                        self.data['raw_cpu_usr'] = float(elem[1])
                        self.data['raw_cpu_nice'] = float(elem[2])
                        self.data['raw_cpu_sys'] = float(elem[3])
                        self.data['raw_cpu_idle'] = float(elem[4])
                        self.data['raw_cpu_iowait'] = float(elem[5])
                    elif line.startswith("page"):
                        elem = line.split()
                        self.data['raw_blocks_in'] = float(elem[1])
                        self.data['raw_blocks_out'] = float(elem[2])
                    elif line.startswith('swap'):
                        elem = line.split()
                        self.data['raw_swap_in'] = float(elem[1])
                        self.data['raw_swap_out'] = float(elem[2])
                    elif line.startswith('intr'):
                        elem = line.split()
                        self.data['raw_interrupts'] = float(elem[1])
                    elif line.startswith('ctxt'):
                        elem = line.split()
                        self.data['raw_context_switches'] = float(elem[1])
                    line = fd.readline()
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/stat")
            return
        try:
            # blocks_in/out and swap_in/out are moved to /proc/vmstat in >2.5 kernels
            with open('/proc/vmstat') as fd:
                line = fd.readline()
                while line != '':
                    elem = re.split(r"\s+", line)
                    if line.startswith("pgpgin "):
                        self.data['raw_blocks_in'] = float(elem[1])
                    elif line.startswith("pgpgout "):
                        self.data['raw_blocks_out'] = float(elem[1])
                    elif line.startswith("pswpin "):
                        self.data['raw_swap_in'] = float(elem[1])
                    elif line.startswith("pswpout "):
                        self.data['raw_swap_out'] = float(elem[1])
                    line = fd.readline()
        except IOError as ex:
            self.logger.log(Logger.NOTICE, "ProcInfo: cannot open /proc/vmstat")


    def readMemInfo(self):
        """ sizes are reported in MB (except _usage that is in percent). """
        try:
            with open('/proc/meminfo') as fd:
                line = fd.readline()
                while line != '':
                    elem = re.split(r"\s+", line)
                    if line.startswith("MemFree:"):
                        self.data['mem_free'] = float(elem[1]) // 1024.0
                    if line.startswith("MemTotal:"):
                        self.data['total_mem'] = float(elem[1]) // 1024.0
                    if line.startswith("SwapFree:"):
                        self.data['swap_free'] = float(elem[1]) // 1024.0
                    if line.startswith("SwapTotal:"):
                        self.data['total_swap'] = float(elem[1]) // 1024.0
                    if line.startswith("Buffers:"):
                        self.data['mem_buffers'] = float(elem[1]) // 1024.0
                    if line.startswith("Cached:"):
                        self.data['mem_cached'] = float(elem[1]) // 1024.0
                    line = fd.readline()
            if 'mem_free' in self.data and 'mem_buffers' in self.data and 'mem_cached' in self.data:
                self.data['mem_actualfree'] = self.data['mem_free'] + self.data['mem_buffers'] + self.data['mem_cached']
            if 'total_mem' in self.data and 'mem_actualfree' in self.data:
                self.data['mem_used'] = self.data['total_mem'] - self.data['mem_actualfree']
            if 'total_swap' in self.data and 'swap_free' in self.data:
                self.data['swap_used'] = self.data['total_swap'] - self.data['swap_free']
            if 'mem_used' in self.data and 'total_mem' in self.data and self.data['total_mem'] > 0:
                self.data['mem_usage'] = 100.0 * self.data['mem_used'] / self.data['total_mem']
            if 'swap_used' in self.data and 'total_swap' in self.data and self.data['total_swap'] > 0:
                self.data['swap_usage'] = 100.0 * self.data['swap_used'] / self.data['total_swap']
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/meminfo")
            return


    def readLoadAvg(self):
        """ read system load average """
        try:
            with open('/proc/loadavg') as fd:
                line = fd.readline()
            elem = re.split(r"\s+", line)
            self.data['load1'] = float(elem[0])
            self.data['load5'] = float(elem[1])
            self.data['load15'] = float(elem[2])
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/meminfo")
            return


    def darwin_readLoadAvg(self):
        """ read system load average on Darwin """
        try:
            loadAvg = os.popen('sysctl vm.loadavg')
            line = loadAvg.readline()
            loadAvg.close()
            elem = re.split(r"\s+", line)
            self.data['load1'] = float(elem[1])
            self.data['load5'] = float(elem[2])
            self.data['load15'] = float(elem[3])
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot run 'sysctl vm.loadavg")
            return


    def countProcesses(self):
        """
        read the number of processes currently running on the system
        # old version
        nr = 0
        try:
            for file in os.listdir("/proc"):
                if re.match(r"\\d+", file):
                    nr += 1
            self.data['processes'] = nr
        except IOError, ex:
            self.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc to count processes")
            return
        """
        # new version
        total = 0
        states = {'D': 0, 'R': 0, 'S': 0, 'T': 0, 'Z': 0}
        try:
            output = os.popen('ps -A -o state')
            line = output.readline()
            while line != '':
                if line[0] in states:
                    states[line[0]] = states[line[0]] + 1
                else:
                    states[line[0]] = 1
                total = total + 1
                line = output.readline()
            output.close()
            self.data['processes'] = total
            for key in list(states.keys()):
                self.data['processes_'+key] = states[key]
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot get output from ps command")
            return


    def readGenericInfo(self):
        """ reads the IP, hostname, cpu_MHz, uptime """
        self.data['hostname'] = socket.getfqdn()
        try:
            output = os.popen('/sbin/ifconfig -a')
            eth, ip = '', ''
            line = output.readline()
            while line != '':
                line = line.strip()
                if line.startswith("eth"):
                    elem = line.split()
                    eth = elem[0]
                    ip = ''
                if len(eth) > 0 and line.startswith("inet addr:"):
                    ip = re.match(r"inet addr:(\d+\.\d+\.\d+\.\d+)", line).group(1)
                    self.data[eth + '_ip'] = ip
                    eth = ''
                line = output.readline()
            output.close()
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot get output from /sbin/ifconfig -a")

        try:
            noCpus = 0
            with open('/proc/cpuinfo') as fd:
                line = fd.readline()
                while line != '':
                    if line.startswith("cpu MHz"):
                        self.data['cpu_MHz'] = float(re.match(r"cpu MHz\s+:\s+(\d+\.?\d*)", line).group(1))
                        noCpus += 1

                    if line.startswith("vendor_id"):
                        self.data['cpu_vendor_id'] = re.match(r"vendor_id\s+:\s+(.+)", line).group(1)

                    if line.startswith("cpu family"):
                        self.data['cpu_family'] = re.match(r"cpu family\s+:\s+(.+)", line).group(1)

                    if line.startswith("model") and not line.startswith("model name"):
                        self.data['cpu_model'] = re.match(r"model\s+:\s+(.+)", line).group(1)

                    if line.startswith("model name"):
                        self.data['cpu_model_name'] = re.match(r"model name\s+:\s+(.+)", line).group(1)

                    if line.startswith("bogomips"):
                        self.data['bogomips'] = float(re.match(r"bogomips\s+:\s+(\d+\.?\d*)", line).group(1))

                    line = fd.readline()
            self.data['no_CPUs'] = noCpus
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/cpuinfo")

        # try to determine the kernel version
        try:
            output = os.popen('uname -r')
            line = output.readline().replace("\n", "")
            self.data['kernel_version'] = line
            output.close()
        except IOError as ex:
            self.logger.log(Logger.ERROR, "ProcInfo: cannot get kernel version with 'uname -r'")

        # try to determine the platform
        try:
            output = os.popen('uname -m 2>/dev/null || uname')
            line = output.readline().replace("\n", "")
            self.data['platform'] = line
            output.close()
        except IOError as ex:
            self.logger.log(Logger.ERROR, "ProcInfo: cannot get platform with 'uname -m'")

        # try to determine the OS type
        osType = None
        try:
            output = os.popen('env PATH# pylint: disable=line-too-long=$PATH:/bin:/usr/bin lsb_release -d 2>/dev/null')
            line = output.readline().replace("\n", "")
            mo = re.match(r"Description:\s*(.*)", line)
            if not mo is None:
                osType = mo.group(1)
            output.close()
        except IOError as ex:
            pass
        # if lsb_release didn't work, try again...
        if osType is None:
            for fileName in ["/etc/redhat-release", "/etc/debian_version",
                             "/etc/SuSE-release", "/etc/slackware-version",
                             "/etc/gentoo-release", "/etc/mandrake-release",
                             "/etc/mandriva-release", "/etc/issue"]:
                try:
                    with open(fileName) as fd:
                        line = fd.readline().replace("\n", "")
                        if len(line) > 0:
                            osType = line
                    break
                except IOError as ex:
                    pass
        # if none of these, try just uname -s
        if osType is None:
            try:
                output = os.popen('uname -s')
                line = output.readline().replace("\n", "")
                osType = line
                output.close()
            except IOError as ex:
                pass
        if osType is not None:
            self.data['os_type'] = osType
        else:
            self.logger.log(Logger.ERROR, "ProcInfo: cannot determine operating system type")


    def readUptimeAndLoadAvg(self):
        """
        read system's uptime and load average. Time is reported as a floating number, in days.
        It uses the 'uptime' command which's output looks like these:
        19:55:37 up 11 days, 18:57,  1 user,  load average: 0.00, 0.00, 0.00
        18:42:31 up 87 days, 18:10,  9 users,  load average: 0.64, 0.84, 0.80
        6:42pm  up 7 days  3:08,  7 users,  load average: 0.18, 0.14, 0.10
        6:42pm  up 33 day(s),  1:54,  1 user,  load average: 0.01, 0.00, 0.00
        18:42  up 7 days,  3:45, 2 users, load averages: 1.10 1.11 1.06
        18:47:41  up 7 days,  4:35, 19 users,  load average: 0.66, 0.44, 0.41
        11:57am  up   2:21,  22 users,  load average: 0.59, 0.93, 0.73
        """
        try:
            output = os.popen('uptime')
            line = output.readline().replace("\n", "")
            mo = re.match(r".*up\s+((\d+)\s+day[ (s),]+)?(\d+)(:(\d+))?[^\d]+(\d+)[^\d]+([\d\.]+)[^\d]+([\d\.]+)[^\d]+([\d\.]+)", line)
            if mo is None:
                self.logger.log(Logger.ERROR, "ProcInfo: got nonparsable output from uptime: "+line)
                return
            (days, hour, mins, users, load1, load5, load15) = (mo.group(2), mo.group(3), mo.group(5), float(mo.group(6)), mo.group(7), mo.group(8), mo.group(9))
            if days is None:
                days = 0.0
            if mins is None:
                (mins, hour) = (hour, 0.0)
            uptime = float(days) + float(float(hour) // 24.0) + float(float(mins) // 1440.0)
            self.data['uptime'] = float(uptime)
            self.data['logged_users'] = float(users)  # this is currently not reported
            self.data['load1'] = float(load1)
            self.data['load5'] = float(load5)
            self.data['load15'] = float(load15)
            output.close()
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot get output from uptime command")

    @staticmethod
    def diffWithOverflowCheck(new, old):
        """
        do a difference with overflow check and repair
        the counter is unsigned 32 or 64 bit
        """
        if new >= old:
            return new - old
        else:
            maxv = (1 << 31) * 2  # 32 bits
            if old >= maxv:
                maxv = (1 << 63) * 2  # 64 bits
            return new - old + maxv


    def readNetworkInfo(self):
        """ read network information like transfered kBps and nr. of errors on each interface """
        try:
            with open('/proc/net/dev') as fd:
                line = fd.readline()
                while line != '':
                    m = re.match(r"\s*eth(\d):(\d+)\s+\d+\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+\d+\s+(\d+)", line)
                    if m is not None:
                        self.data['raw_eth'+m.group(1)+'_in'] = float(m.group(2))
                        self.data['raw_eth'+m.group(1)+'_out'] = float(m.group(4))
                        self.data['raw_eth'+m.group(1)+'_errs'] = int(m.group(3)) + int(m.group(5))
                    line = fd.readline()
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/net/dev")
            return


    def readNetStat(self):
        """ run nestat and collect sockets info (tcp, udp, unix) and connection states for tcp sockets from netstat """
        try:
            output = os.popen('netstat -an 2>/dev/null')
            sockets = {'sockets_tcp': 0, 'sockets_udp': 0, 'sockets_unix': 0, 'sockets_icm': 0}
            tcpDetails = {'sockets_tcp_ESTABLISHED': 0, 'sockets_tcp_SYN_SENT': 0,
                          'sockets_tcp_SYN_RECV': 0, 'sockets_tcp_FIN_WAIT1': 0, 'sockets_tcp_FIN_WAIT2': 0,
                          'sockets_tcp_TIME_WAIT': 0, 'sockets_tcp_CLOSED': 0, 'sockets_tcp_CLOSE_WAIT': 0,
                          'sockets_tcp_LAST_ACK': 0, 'sockets_tcp_LISTEN': 0, 'sockets_tcp_CLOSING': 0,
                          'sockets_tcp_UNKNOWN': 0}
            line = output.readline()
            while line != '':
                arg = line.split()
                proto = arg[0]
                if proto.find('tcp') == 0:
                    sockets['sockets_tcp'] += 1
                    state = arg[len(arg)-1]
                    key = 'sockets_tcp_'+state
                    if key in tcpDetails:
                        tcpDetails[key] += 1
                if proto.find('udp') == 0:
                    sockets['sockets_udp'] += 1
                if proto.find('unix') == 0:
                    sockets['sockets_unix'] += 1
                if proto.find('icm') == 0:
                    sockets['sockets_icm'] += 1

                line = output.readline()
            output.close()

            for key in list(sockets.keys()):
                self.data[key] = sockets[key]
            for key in list(tcpDetails.keys()):
                self.data[key] = tcpDetails[key]
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot get output from netstat command")
            return

    ##############################################################################################
    # job monitoring related functions
    ##############################################################################################

    def getChildren(self, parent):
        """ internal function that gets the full list of children (pids) for a process (pid) """
        pidmap = {}
        try:
            output = os.popen('ps -A -o "pid ppid"')
            line = output.readline()  # skip headers
            line = output.readline()
            while line != '':
                line = line.strip()
                elem = re.split(r"\s+", line)
                pidmap[int(elem[0])] = int(elem[1])
                line = output.readline()
            output.close()
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot execute ps -A -o \"pid ppid\"")

        if parent not in pidmap:
            self.logger.log(Logger.INFO, 'ProcInfo: No job with pid='+str(parent))
            self.removeJobToMonitor(parent)
            return []

        children = [parent]
        i = 0
        while i < len(children):
            prnt = children[i]
            for (pid, ppid) in list(pidmap.items()):
                if ppid == prnt:
                    children.append(pid)
                i += 1
        return children

    @staticmethod
    def parsePSTime(myTime):
        """
        internal function that parses a time formatted like "days-hours:min:sec" and returns the corresponding
        number of seconds.
        """
        myTime = myTime.strip()
        m = re.match(r"(\d+)-(\d+):(\d+):(\d+)", myTime)
        if m is not None:
            return int(m.group(1)) * 24 * 3600 + int(m.group(2)) * 3600 + int(m.group(3)) * 60 + int(m.group(4))
        else:
            m = re.match(r"(\d+):(\d+):(\d+)", myTime)
            if m is not None:
                return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
            else:
                m = re.match(r"(\d+):(\d+)", myTime)
                if m is not None:
                    return int(m.group(1)) * 60 + int(m.group(2))
                else:
                    return None


    def readJobInfo(self, pid):
        """
        read information about this the JOB_PID process
        memory sizes are given in KB
        """
        if (pid == '') or pid not in self.jobs:
            return
        children = self.getChildren(pid)
        if len(children) == 0:
            self.logger.log(Logger.INFO, "ProcInfo: Job with pid="+str(pid)+" terminated; removing it from monitored jobs.")
            # print ":("
            self.removeJobToMonitor(pid)
            return
        try:
            jStatus = os.popen("ps --no-headers --pid " + ",".join([repr(child) for child in  children]) + " -o pid,etime,time,%cpu,%mem,rsz,vsz,comm")
            memCmdMap = {}
            etime, cputime, pcpu, pmem, rsz, vsz, dummycomm, fd = None, None, None, None, None, None, None, None
            line = jStatus.readline()
            while line != '':
                line = line.strip()
                m = re.match(r"(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.+)", line)
                if m != None:
                    apid, etime1, cputime1, pcpu1 = m.group(1), m.group(2), m.group(3), m.group(4)
                    pmem1, rsz1, vsz1, comm1 = m.group(5), m.group(6), m.group(7), m.group(8)
                    sec = self.parsePSTime(etime1)
                    if (sec is not None) and (sec > self.addIfValid(etime, 0)):  # the elapsed time is the maximum of all elapsed
                        etime = sec
                    sec = self.parsePSTime(cputime1)		# times corespornding to all child processes.
                    cputime = self.addIfValid(cputime, sec)  # total cputime is the sum of cputimes for all processes.
                    pcpu = self.addIfValid(pcpu, float(pcpu1))  # total %cpu is the sum of all children %cpu.
                    if repr(pmem1)+" "+repr(rsz1)+" "+repr(vsz1)+" "+repr(comm1) not in memCmdMap:
                        # it's the first thread/process with this memory footprint; add it.
                        memCmdMap[repr(pmem1)+" "+repr(rsz1)+" "+repr(vsz1)+" "+repr(comm1)] = 1
                        pmem = self.addIfValid(pmem, float(pmem1))
                        rsz = self.addIfValid(rsz, int(rsz1))
                        vsz = self.addIfValid(vsz, int(vsz1))
                        fd = self.addIfValid(fd, self.countOpenFD(apid))
                    # else not adding memory usage
                line = jStatus.readline()
            jStatus.close()
            if etime is not None:
                self.jobs[pid]['DATA']['run_time'] = etime
            if cputime is not None:
                self.jobs[pid]['DATA']['cpu_time'] = cputime
            if pcpu is not None:
                self.jobs[pid]['DATA']['cpu_usage'] = pcpu
            if pmem is not None:
                self.jobs[pid]['DATA']['mem_usage'] = pmem
            if rsz is not None:
                self.jobs[pid]['DATA']['rss'] = rsz
            if vsz is not None:
                self.jobs[pid]['DATA']['virtualmem'] = vsz
            if fd is not None:
                self.jobs[pid]['DATA']['open_files'] = fd
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ProcInfo: cannot execute ps --no-headers -eo \"pid ppid\"")

    @staticmethod
    def addIfValid(sumV, toAdd):
        """
        return the sum of the two given parameters (or None if the case)
        """
        if toAdd is None:
            return sumV
        if sumV is None:
            return toAdd
        return sumV + toAdd


    def countOpenFD(self, pid):
        """ count the number of open files for the given pid """
        dirPath = '/proc/'+str(pid)+'/fd'
        if os.access(dirPath, os.F_OK):
            if os.access(dirPath, os.X_OK):
                listDirs = os.listdir(dirPath)
                openFiles = len(listDirs)
                if pid == os.getpid():
                    openFiles -= 2
                self.logger.log(Logger.DEBUG, "Counting open_files for "+repr(pid)+": "+str(len(listDirs))+" => "+repr(openFiles)+" open_files")
                return openFiles
            else:
                self.logger.log(Logger.ERROR, "ProcInfo: cannot count the number of opened files for job "+repr(pid))
        else:
            self.logger.log(Logger.ERROR, "ProcInfo: job "+repr(pid)+" dosen't exist")
        # failed
        return None


    def readJobDiskUsage(self, pid):
        """
        if there is an work directory defined, then compute the used space in that directory
        and the free disk space on the partition to which that directory belongs
        sizes are given in MB
        """
        if (pid == '') or pid not in self.jobs:
            return
        workDir = self.jobs[pid]['WORKDIR']
        if workDir == '':
            return
        try:
            duOutput = os.popen("du -Lsck " + workDir + " | tail -1 | cut -f 1")
            line = duOutput.readline()
            self.jobs[pid]['DATA']['workdir_size'] = int(line) // 1024.0
        except IOError as ex:
            del ex
            self.logger.log(Logger.ERROR, "ERROR", "ProcInfo: cannot run du to get job's disk usage for job "+repr(pid))
        try:
            dfOutput = os.popen("df -k "+workDir+" | tail -1")
            line = dfOutput.readline().strip()
            m = re.match(r"\S+\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)%", line)
            if m != None:
                self.jobs[pid]['DATA']['disk_total'] = float(m.group(1)) // 1024.0
                self.jobs[pid]['DATA']['disk_used'] = float(m.group(2)) // 1024.0
                self.jobs[pid]['DATA']['disk_free'] = float(m.group(3)) // 1024.0
                self.jobs[pid]['DATA']['disk_usage'] = float(m.group(4)) // 1024.0
            dfOutput.close()
        except IOError as ex:
            self.logger.log(Logger.ERROR, "ERROR", "ProcInfo: cannot run df to get job's disk usage for job "+repr(pid))


    def computeCummulativeParams(self, dataRef, prevDataRef):
        """ create cummulative parameters based on raw params like cpu_, blocks_, swap_, or ethX_ """
        if prevDataRef == {}:
            for key in list(dataRef.keys()):
                if key.find('raw_') == 0:
                    prevDataRef[key] = dataRef[key]
            prevDataRef['TIME'] = dataRef['TIME']
            return

        # cpu -related params
        if ('raw_cpu_usr' in dataRef) and ('raw_cpu_usr' in prevDataRef):
            diff = {}
            cpuSum = 0
            for param in ['cpu_usr', 'cpu_nice', 'cpu_sys', 'cpu_idle', 'cpu_iowait']:
                diff[param] = self.diffWithOverflowCheck(dataRef['raw_'+param], prevDataRef['raw_'+param])
                cpuSum += diff[param]
            for param in ['cpu_usr', 'cpu_nice', 'cpu_sys', 'cpu_idle', 'cpu_iowait']:
                if cpuSum != 0:
                    dataRef[param] = 100.0 * diff[param] / cpuSum
                else:
                    del dataRef[param]
            if cpuSum != 0:
                dataRef['cpu_usage'] = 100.0 * (cpuSum - diff['cpu_idle']) / cpuSum
            else:
                del dataRef['cpu_usage']
            # add the other parameters
            for param in ['interrupts', 'context_switches']:
                if 'raw_'+param in prevDataRef and 'raw_'+param in dataRef:
                    dataRef[param] = self.diffWithOverflowCheck(dataRef['raw_'+param], prevDataRef['raw_'+param])
                else:
                    del dataRef[param]

        # swap, blocks, context switches, interrupts
        interval = dataRef['TIME'] - prevDataRef['TIME']
        for param in ['blocks_in', 'blocks_out', 'swap_in', 'swap_out', 'interrupts', 'context_switches']:
            if (interval != 0) and 'raw_'+param in prevDataRef and 'raw_'+param in dataRef:
                diff = self.diffWithOverflowCheck(dataRef['raw_'+param], prevDataRef['raw_'+param])
                dataRef[param+'_R'] = diff // interval
            else:
                del dataRef[param+'_R']

        # eth - related params
        for rawParam in list(dataRef.keys()):
            if (rawParam.find('raw_eth') == 0) and rawParam in prevDataRef:
                param = rawParam.split('raw_')[1]
                if interval != 0:
                    dataRef[param] = self.diffWithOverflowCheck(dataRef[rawParam], prevDataRef[rawParam])  # absolute difference
                    if param.find('_errs') == -1:
                        dataRef[param] = dataRef[param] / interval / 1024.0 # if it's _in or _out, compute in KB/sec
                else:
                    del dataRef[param]

        # copy contents of the current data values to the
        for param in list(dataRef.keys()):
            if param.find('raw_') == 0:
                prevDataRef[param] = dataRef[param]
        prevDataRef['TIME'] = dataRef['TIME']


    def getFilteredData(self, dataHash, paramsList, prevDataHash=None):
        """ Return a hash containing (param,value) pairs with existing values from the requested ones """

        if prevDataHash is not None:
            self.computeCummulativeParams(dataHash, prevDataHash)

        result = {}
        for param in paramsList:
            if param == 'net_sockets':
                for key in list(dataHash.keys()):
                    if key.find('sockets') == 0 and key.find('sockets_tcp_') == -1:
                        result[key] = dataHash[key]
            elif param == 'net_tcp_details':
                for key in list(dataHash.keys()):
                    if key.find('sockets_tcp_') == 0:
                        result[key] = dataHash[key]

            m = re.match(r"^net_(.*)$", param)
            if m is None:
                m = re.match(r"^(ip)$", param)
            if m is not None:
                netParam = m.group(1)
                # self.logger.log(Logger.DEBUG, "Querying param "+net_param)
                for key, value in list(dataHash.items()):
                    m = re.match(r"eth\d_"+netParam, key)
                    if m is not None:
                        result[key] = value
            else:
                if param == 'processes':
                    for key in list(dataHash.keys()):
                        if key.find('processes') == 0:
                            result[key] = dataHash[key]
                elif param in dataHash:
                    result[param] = dataHash[param]
        sortedResult = []
        keys = sorted(result.keys())
        for key in keys:
            sortedResult.append((key, result[key]))
        return sortedResult

######################################################################################
# self test

def main():
    """ Main function """
    logger = Logger(Logger.DEBUG)
    pi = ProcInfo(logger)

    print("first update")
    pi.update()
    print("Sleeping to accumulate")
    time.sleep(1)
    pi.update()

    print("System Monitoring:")
    sysCpuParams = ['cpu_usr', 'cpu_sys', 'cpu_idle', 'cpu_nice', 'cpu_usage', 'context_switches', 'interrupts']
    sysIoParams = ['blocks_in', 'blocks_out', 'swap_in', 'swap_out']
    sysMemParams = ['mem_used', 'mem_free', 'total_mem', 'mem_usage']
    sysSwapParams = ['swap_used', 'swap_free', 'total_swap', 'swap_usage']
    sysLoadParams = ['load1', 'load5', 'load15', 'processes', 'uptime']
    sysGenParams = ['hostname', 'cpu_MHz', 'no_CPUs', 'cpu_vendor_id', 'cpu_family', 'cpu_model', 'cpu_model_name', 'bogomips']
    sysNetParams = ['net_in', 'net_out', 'net_errs', 'ip']
    sysNetStat = ['sockets_tcp', 'sockets_udp', 'sockets_unix', 'sockets_icm']
    sysTcpDetails = ['sockets_tcp_ESTABLISHED', 'sockets_tcp_SYN_SENT', 'sockets_tcp_SYN_RECV', 'sockets_tcp_FIN_WAIT1',
                     'sockets_tcp_FIN_WAIT2', 'sockets_tcp_TIME_WAIT', 'sockets_tcp_CLOSED', 'sockets_tcp_CLOSE_WAIT',
                     'sockets_tcp_LAST_ACK', 'sockets_tcp_LISTEN', 'sockets_tcp_CLOSING', 'sockets_tcp_UNKNOWN']

    print("sys_cpu_params", pi.getSystemData(sysCpuParams))
    print("sys_io_params", pi.getSystemData(sysIoParams))
    print("sys_mem_params", pi.getSystemData(sysMemParams))
    print("sys_swap_params", pi.getSystemData(sysSwapParams))
    print("sys_load_params", pi.getSystemData(sysLoadParams))
    print("sys_gen_params", pi.getSystemData(sysGenParams))
    print("sys_net_params", pi.getSystemData(sysNetParams))
    print("sys_net_stat", pi.getSystemData(sysNetStat))
    print("sys_tcp_details", pi.getSystemData(sysTcpDetails))

    jobPid = os.getpid()

    print("Job (mysefl) monitoring:")
    pi.addJobToMonitor(jobPid, os.getcwd())
    print("Sleep another second")
    time.sleep(1)
    pi.update()

    jobCpuParams = ['run_time', 'cpu_time', 'cpu_usage']
    jobMemParams = ['mem_usage', 'rss', 'virtualmem', 'open_files']
    jobDiskParams = ['workdir_size', 'disk_used', 'disk_free', 'disk_total', 'disk_usage']
    time.sleep(10)
    print("job_cpu_params", pi.getJobData(jobPid, jobCpuParams))
    print("job_mem_params", pi.getJobData(jobPid, jobMemParams))
    print("job_disk_params", pi.getJobData(jobPid, jobDiskParams))

    pi.removeJobToMonitor(os.getpid())

if __name__ == '__main__':
    main()
