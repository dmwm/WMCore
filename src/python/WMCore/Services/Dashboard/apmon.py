# pylint: disable=line-too-long
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

This is a python implementation for the ApMon API for sending
data to the MonALISA service.

For further details about ApMon please see the C/C++ or Java documentation
You can find a sample usage of this module in apmTest.py.

Note that the parameters must be either integers(32 bits) or doubles(64 bits).
Sending strings is supported, but they will not be stored in the
farm's store nor shown in the farm's window in the MonALISA client.
"""
from __future__ import division
import re
import xdrlib
import socket
import struct
import io
import threading
import time
import random
import copy
import os

from types import LongType

from WMCore.Services.Dashboard import ProcInfo
from WMCore.Services.Dashboard.Logger import Logger
# __all__ = ["ApMon"]

# __debug = False # set this to True to be verbose


class ApMon(object):
    """
    Main class for sending monitoring data to a MonaLisa module.
    One or more destinations can be chosen for the data. See constructor.

    The data is packed in UDP datagrams, using XDR. The following fields are sent:
    - version & password(string)
    - cluster name(string)
    - node name(string)
    - number of parameters(int)
    - for each parameter:
        - name(string)
        - value type(int)
        - value
    - optionally a(int) with the given timestamp

    Attributes(public):
    - destinations - a list containing(ip, port, password) tuples
    - configAddresses - list with files and urls from where the config is read
    - configRecheckInterval - period, in seconds, to check for changes
      in the configAddresses list
    - configRecheck - boolean - whether to recheck periodically for changes
      in the configAddresses list
    """

    __defaultOptions = {
        'job_monitoring': True,       # perform(or not) job monitoring
        'job_interval'	: 120,         # at this interval(in seconds)
        'job_data_sent' : 0,          # time from Epoch when job information was sent; don't touch!

        'job_cpu_time'  : True,       # elapsed time from the start of this job in seconds
        'job_run_time'  : True,       # processor time spent running this job in seconds
        'job_cpu_usage' : True,       # current percent of the processor used for this job, as reported by ps
        'job_virtualmem': True,       # size in JB of the virtual memory occupied by the job, as reported by ps
        'job_rss'       : True,       # size in KB of the resident image size of the job, as reported by ps
        'job_mem_usage' : True,       # percent of the memory occupied by the job, as reported by ps
        'job_workdir_size': True,     # size in MB of the working directory of the job
        'job_disk_total': True,       # size in MB of the total size of the disk partition containing the working directory
        'job_disk_used' : True,       # size in MB of the used disk partition containing the working directory
        'job_disk_free' : True,       # size in MB of the free disk partition containing the working directory
        'job_disk_usage': True,       # percent of the used disk partition containing the working directory
        'job_open_files': True,       # number of open file descriptors

        'sys_monitoring': True,       # perform(or not) system monitoring
        'sys_interval'  : 120,         # at this interval(in seconds)
        'sys_data_sent' : 0,          # time from Epoch when system information was sent; don't touch!

        'sys_cpu_usr'   : True,      # cpu-usage information
        'sys_cpu_sys'   : True,      # all these will produce coresponding paramas without "sys_"
        'sys_cpu_nice'  : True,
        'sys_cpu_idle'  : True,
        'sys_cpu_iowait': True,
        'sys_cpu_usage' : True,
        'sys_interrupts_R': True,
        'sys_context_switches_R' : True,
        'sys_load1'     : True,       # system load information
        'sys_load5'     : True,
        'sys_load15'    : True,
        'sys_mem_used'  : True,      # memory usage information
        'sys_mem_free'  : True,
        'sys_mem_actualfree': True,  # actually free amount of mem: free + buffers + cached
        'sys_mem_usage' : True,
        'sys_mem_buffers':True,
        'sys_mem_cached': True,
        'sys_blocks_in_R' : True,
        'sys_blocks_out_R': True,
        'sys_swap_used' : True,       # swap usage information
        'sys_swap_free' : True,
        'sys_swap_usage': True,
        'sys_swap_in_R'   : True,
        'sys_swap_out_R'  : True,
        'sys_net_in'    : True,       # network transfer in kBps
        'sys_net_out'   : True,       # these will produce params called ethX_in, ethX_out, ethX_errs
        'sys_net_errs'  : True,      # for each eth interface
        'sys_net_sockets' : True,     # number of opened sockets for each proto => sockets_tcp/udp/unix ...
        'sys_net_tcp_details' : True, # number of tcp sockets in each state => sockets_tcp_LISTEN, ...
        'sys_processes' : True,
        'sys_uptime'    : True,       # uptime of the machine, in days(float number)

        'general_info'  : True,       # send(or not) general host information once every 2 x $sys_interval seconds
        'general_data_sent': 0,       # time from Epoch when general information was sent; don't touch!

        'hostname'      : True,
        'ip'            : True,       # will produce ethX_ip params for each interface
        'cpu_MHz'       : True,
        'no_CPUs'       : True,       # number of CPUs
        'kernel_version': True,
        'platform'      : True,
        'os_type'       : True,
        'total_mem'     : True,
        'total_swap'    : True,
        'cpu_vendor_id'	: True,
        'cpu_family'	: True,
        'cpu_model'	: True,
        'cpu_model_name': True,
        'bogomips'	: True}

    def __init__(self, initValue, defaultLogLevel=Logger.INFO):
        """
        Class constructor:
        - if initValue is a string, put it in configAddresses and load destinations
          from the file named like that. if it starts with "http://", the configuration
          is loaded from that URL. For background monitoring, given parameters will overwrite defaults

        - if initValue is a list, put its contents in configAddresses and create
          the list of destinations from all those sources. For background monitoring,
          given parameters will overwrite defaults(see __defaultOptions)

        - if initValue is a tuple(of strings), initialize destinations with that values.
          Strings in this tuple have this form: "{hostname|ip}[:port][ passwd]", the
          default port being 8884 and the default password being "". Background monitoring will be
          enabled sending the parameters active from __defaultOptions(see end of file)

        - if initValue is a hash(key = string(hostname|ip[:port][ passwd]),
          val = hash{'param_name': True/False, ...}) the given options for each destination
          will overwrite the default parameters(see __defaultOptions)
        """
        self.destinations = {}              # empty, by default; key = tuple(host, port, pass) ; val = hash {"param_mame" : True/False, ...}
        self.destPrevData = {}              # empty, by defaul; key = tuple(host, port, pass) ; val = hash {"param_mame" : value, ...}
        self.senderRef = {}                 # key = tuple(host, port, pass); val = hash {'INSTANCE_ID', 'SEQ_NR' }
        self.configAddresses = []           # empty, by default; list of files/urls from where we read config
        self.configRecheckInterval = 600    # 10 minutes
        self.configRecheck = True           # enabled by default
        self.performBgMonitoring = True     # by default, perform background monitoring
        self.monitoredJobs = {}	            # Monitored jobs; key = pid; value = hash with
        self.maxMsgRate = 100                # Maximum number of messages allowed to be sent per second
        self.maxMsgSize = 1440              # Maximum size of a message. Bulk parameters are split in several messages of smaller size
        self.__defaultSenderRef = {'INSTANCE_ID': self.__getInstanceID(), 'SEQ_NR': 0}
        self.__defaultUserCluster = "ApMon_UserSend"
        self.__defaultUserNode = socket.getfqdn()
        self.__defaultSysMonCluster = "ApMon_SysMon"
        self.__defaultSysMonNode = socket.getfqdn()
        # don't touch these:
        self.__freed = False
        self.__udpSocket = None
        self.__configUpdateLock = threading.Lock()
        self.__configUpdateEvent = threading.Event()
        self.__configUpdateFinished = threading.Event()
        self.__bgMonitorLock = threading.Lock()
        self.__bgMonitorEvent = threading.Event()
        self.__bgMonitorFinished = threading.Event()
        # don't allow a user to send more than MAX_MSG messages per second, in average
        self.__crtTime = 0
        self.__prvTime = 0
        self.__prvSent = 0
        self.__prvDrop = 0
        self.__crtSent = 0
        self.__crtDrop = 0
        self.__hWeight = 0.95
        try:
            self.logger = Logger(defaultLogLevel)
        except:
            self.logger = defaultLogLevel
        try:
            self.setDestinations(initValue)
            self.__udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            if len(self.configAddresses) > 0:
                # if there are addresses that need to be monitored,
                # start config checking and reloading thread
                th = threading.Thread(target=self.__configLoader)
                th.setDaemon(True)  # this is a daemon thread
                th.start()
            # create the ProcInfo instance
            self.procInfo = ProcInfo.ProcInfo(self.logger)
            # self.procInfo.update()
            # start the background monitoring thread
            th = threading.Thread(target=self.__bgMonitor)
            th.setDaemon(True)
            th.start()
        except Exception as msg:
            self.logger.log(Logger.ERROR, "Error initializing ApMon "+str(msg), True)

    def sendParams(self, params):
        """
        Send multiple parameters to MonALISA, with default(last given) cluser and node names.
        """
        self.sendTimedParams(-1, params)

    def sendTimedParams(self, timeStamp, params):
        """
        Send multiple parameters, specifying the time for them, with default(last given) cluster and node names.
        (See sendTimedParameters for more details)
        """
        self.sendTimedParameters(None, None, timeStamp, params)

    def sendParameter(self, clusterName, nodeName, paramName, paramValue):
        """
        Send a single parameter to MonALISA.
        """
        self.sendTimedParameter(clusterName, nodeName, -1, paramName, paramValue)

    def sendTimedParameter(self, clusterName, nodeName, timeStamp, paramName, paramValue):
        """
        Send a single parameter, with a given time.
        """
        self.sendTimedParameters(clusterName, nodeName, timeStamp, {paramName: paramValue})

    def sendParameters(self, clusterName, nodeName, params):
        """
        Send multiple parameters specifying cluster and node name for them
        """
        self.sendTimedParameters(clusterName, nodeName, -1, params)

    def sendTimedParameters(self, clusterName, nodeName, timeStamp, params):
        """
        Send multiple monitored parameters to MonALISA.

        - clusterName is the name of the cluster being monitored. The first
          time this function is called, this paramenter must not be None. Then,
          it can be None; last given clusterName will be used instead.
        - nodeName is the name of the node for which are the parameters. If this
          is None, the full hostname of this machine will be sent instead.
        - timeStamp, if > 0, is given time for the parameters. This is in seconds from Epoch.
          Note that this option should be used only if you are sure about the time for the result.
          Otherwize, the parameters will be assigned a correct time(obtained from NTP servers)
          in MonALISA service. This option can be usefull when parsing logs, for example.
        - params is a dictionary containing pairs with:
            - key: parameter name
            - value: parameter value, either int or float.
          or params is a vector of tuples(key, value). This version can be used
          in case you want to send the parameters in a given order.

        NOTE that python doesn't know about 32-bit floats(only 64-bit floats!)
        """
        try:
            if clusterName is None or clusterName == "":
                clusterName = self.__defaultUserCluster
            else:
                if isinstance(clusterName, int):
                    clusterName = str(clusterName)
                self.__defaultUserCluster = clusterName
            if nodeName is None:
                nodeName = self.__defaultUserNode
            else:
                if isinstance(nodeName, int):
                    nodeName = str(nodeName)
                self.__defaultUserNode = nodeName
            if len(self.destinations) == 0:
                self.logger.log(Logger.WARNING, "Not sending parameters since no destination is defined.")
                return
            self.__configUpdateLock.acquire()
            for dest in list(self.destinations.keys()):
                self.__directSendParams(dest, clusterName, nodeName, timeStamp, params)
            self.__configUpdateLock.release()
        except Exception as msg:
            self.logger.log(Logger.ERROR, "Error in sendTimedParameters: "+str(msg), True)


    def addJobToMonitor(self, pid, workDir, clusterName, nodeName):
        """
        Add a new job to monitor.
        """
        try:
            pid = int(pid)
        except Exception as msg:
            self.logger.log(Logger.ERROR, "Job's PID should be a number: "+str(msg), True)
        try:
            self.__bgMonitorLock.acquire()
            self.monitoredJobs[pid] = {}
            self.monitoredJobs[pid]['CLUSTER_NAME'] = clusterName
            self.monitoredJobs[pid]['NODE_NAME'] = nodeName
            self.procInfo.addJobToMonitor(pid, workDir)
            self.__bgMonitorLock.release()
        except Exception as msg:
            self.logger.log(Logger.ERROR, "Error in addJobToMonitor: "+str(msg), True)

    def removeJobToMonitor(self, pid):
        """
        Remove a job from being monitored.
        """
        try:
            self.__bgMonitorLock.acquire()
            self.procInfo.removeJobToMonitor(pid)
            if pid in self.monitoredJobs:
                del self.monitoredJobs[pid]
            else:
                self.logger.log(Logger.ERROR, "Asked to stop monitoring job that is not monitored; given pid="+str(pid), False)
            self.__bgMonitorLock.release()
        except Exception as msg:
            self.logger.log(Logger.ERROR, "Error in removeJobToMonitor: "+str(msg), True)

    def setMonitorClusterNode(self, clusterName, nodeName):
        """
        Set the cluster and node names where to send system related information.
        """
        self.__bgMonitorLock.acquire()
        if clusterName is not None and clusterName != "":
            self.__defaultSysMonCluster = clusterName
        if nodeName is not None and nodeName != "":
            self.__defaultSysMonNode = nodeName
        self.__bgMonitorLock.release()

    def enableBgMonitoring(self, onOff):
        """
        Enable or disable background monitoring. Note that background monitoring information
        can still be sent if user calls the sendBgMonitoring method.
        """
        self.performBgMonitoring = onOff

    def sendBgMonitoring(self, mustSend=False):
        """
        Send background monitoring about system and jobs to all interested destinations.
        If mustSend == True, the information is sent regardles of the elapsed time since last sent
        If mustSend == False, the data is sent only if the required interval has passed since last sent
        """
        try:
            if len(self.destinations) == 0:
                self.logger.log(Logger.WARNING, "Not sending bg monitoring info since no destination is defined.")
                return
            self.__bgMonitorLock.acquire()
            now = int(time.time())
            updatedProcInfo = False
            for destination, options in list(self.destinations.items()):
                sysParams = []
                jobParams = []
                prevRawData = self.destPrevData[destination]
                # for each destination and its options, check if we have to report any background monitoring data
                if options['sys_monitoring'] and(mustSend or options['sys_data_sent'] + options['sys_interval'] <= now):
                    for param, active in list(options.items()):
                        m = re.match("sys_(.+)", param)
                        if m is not None and active:
                            param = m.group(1)
                            if not(param == 'monitoring' or param == 'interval' or param == 'data_sent'):
                                sysParams.append(param)
                    options['sys_data_sent'] = now
                if options['job_monitoring'] and(mustSend or options['job_data_sent'] + options['job_interval'] <= now):
                    for param, active in list(options.items()):
                        m = re.match("job_(.+)", param)
                        if m is not None and active:
                            param = m.group(1)
                            if not(param == 'monitoring' or param == 'interval' or param == 'data_sent'):
                                jobParams.append(param)
                    options['job_data_sent'] = now
                if options['general_info'] and(mustSend or options['general_data_sent'] + 2 * int(options['sys_interval']) <= now):
                    for param, active in list(options.items()):
                        if not(param.startswith("sys_") or param.startswith("job_")) and active:
                            if not(param == 'general_info' or param == 'general_data_sent'):
                                sysParams.append(param)
                    options['general_data_sent'] = now

                if not updatedProcInfo and ((len(sysParams) > 0) or (len(jobParams) > 0)):
                    self.procInfo.update()
                    updatedProcInfo = True

                sysResults = []
                if len(sysParams) > 0:
                    sysResults = self.procInfo.getSystemData(sysParams, prevRawData)
                if len(sysResults) > 0:
                    self.__directSendParams(destination, self.__defaultSysMonCluster, self.__defaultSysMonNode, -1, sysResults)
                for pid, props in list(self.monitoredJobs.items()):
                    jobResults = []
                    if len(jobParams) > 0:
                        jobResults = self.procInfo.getJobData(pid, jobParams)
                    if len(jobResults) > 0:
                        self.__directSendParams(destination, props['CLUSTER_NAME'], props['NODE_NAME'], -1, jobResults)
            self.__bgMonitorLock.release()
        except Exception as msg:
            self.logger.log(Logger.ERROR, "Error in sendBgMonitoring: "+str(msg), True)

    def setDestinations(self, initValue):
        """
        Set the destinations of the ApMon instance. It accepts the same parameters as the constructor.
        """
        try:
            if isinstance(initValue, basestring):
                self.configAddresses = [initValue]
                self.configRecheck = True
                self.configRecheckInterval = 600
                self.__reloadAddresses()
            elif isinstance(initValue, (list, tuple)):
                self.configAddresses = []
                for dest in initValue:
                    self.__addDestination(dest, self.destinations)
                self.configRecheck = False
            elif isinstance(initValue, dict):
                self.configAddresses = []
                for dest, opts in list(initValue.items()):
                    self.__addDestination(dest, self.destinations, opts)
                self.configRecheck = False
        except Exception as msg:
            self.logger.log(Logger.ERROR, "Error in setDestinations: "+str(msg), True)

    def getConfig(self):
        """
        Returns a multi-line string that contains the configuration of ApMon. This string can
        be passed to the setDestination method(or to the constructor). It has the same
        structure as the config file/url contents.
        """
        conf = ""
        for dest, opts in list(self.destinations.items()):
            h, p, w = dest
            conf += h+":"+str(p)+" "+w+"\n\n"
            ok = sorted(opts.keys())
            for o in ok:
                conf += "xApMon_"+o+" = "+str(opts[o])+"\n"
            conf += "TODO: add the others \n"
        return conf

    def initializedOK(self):
        """
        Returns true if there are destination(s) configured.
        """
        return len(self.destinations) > 0

    def freedOK(self):
        """
        Returns true if all ApMon resources were properly freed.
        """
        return self.__freed

    def setLogLevel(self, strLevel):
        """
        Change the log level. Given level is a string, one of 'FATAL', 'ERROR', 'WARNING',
        'INFO', 'NOTICE', 'DEBUG'.
        """
        self.logger.setLogLevel(strLevel)

    def setMaxMsgRate(self, rate):
        """
        Set the maximum number of messages that can be sent, per second.
        """
        self.maxMsgRate = rate
        self.logger.log(Logger.DEBUG, "Setting maxMsgRate to: " + str(rate))

    def setMaxMsgSize(self, size):
        """
        Set the maximum size of the sent messages. ApMon will try to split in several independent
        messages parameters sent in bulk, if the size would be larger than this
        """
        self.maxMsgSize = size
        self.logger.log(Logger.DEBUG, "Setting maxMsgSize to: %d" % size)

    def free(self):
        """
        Stop background threands, close opened sockets. You have to use this function if you want to
        free all the resources that ApMon takes, and allow it to be garbage-collected.
        """
        try:
            if len(self.configAddresses) > 0:
                self.__configUpdateEvent.set()
                self.__configUpdateFinished.wait()
            self.__bgMonitorEvent.set()
            self.__bgMonitorFinished.wait()

            if self.__udpSocket is not None:
                self.logger.log(Logger.DEBUG, "Closing UDP socket on ApMon object destroy.")
                self.__udpSocket.close()
                self.__udpSocket = None
            self.__freed = True
        except Exception as msg:
            self.logger.log(Logger.ERROR, "Error in free: "+str(msg), True)

    #########################################################################################
    # Internal functions - Config reloader thread
    #########################################################################################

    def __configLoader(self):
        """
        Main loop of the thread that checks for changes and reloads the configuration
        """
        try:
            while not self.__configUpdateEvent.isSet():
                self.__configUpdateEvent.wait(min(30, self.configRecheckInterval))  # don't recheck more often than 30 sec
                if self.__configUpdateEvent.isSet():
                    break
                if self.configRecheck:
                    try:
                        self.__reloadAddresses()
                        self.logger.log(Logger.DEBUG, "Config reloaded. Seleeping for "+repr(self.configRecheckInterval)+" sec.")
                    except Exception as msg:
                        self.logger.log(Logger.ERROR, "Error reloading config: "+str(msg), True)
            self.__configUpdateFinished.set()
        except:
            pass

    def __reloadAddresses(self):
        """
        Refresh now the destinations hash, by loading data from all sources in configAddresses
        """
        newDestinations = {}
        urls = copy.deepcopy(self.configAddresses)
        while len(urls) > 0 and len(newDestinations) == 0:
            src = random.choice(urls)
            urls.remove(src)
            self.__initializeFromFile(src, newDestinations)
        # avoid changing config in the middle of sending packets to previous destinations
        self.__configUpdateLock.acquire()
        self.destinations = newDestinations
        self.__configUpdateLock.release()

    def __addDestination(self, aDestination, tempDestinations, options=__defaultOptions):
        """
        Add a destination to the list.

        aDestination is a string of the form "{hostname|ip}[:port] [passwd]" without quotes.
        If the port is not given, it will be used the default port(8884)
        If the password is missing, it will be considered an empty string
        """
        aDestination = aDestination.strip().replace('\t', ' ')
        while aDestination != aDestination.replace('  ', ' '):
            aDestination = aDestination.replace('  ', ' ')
        sepPort = aDestination.find(':')
        sepPasswd = aDestination.rfind(' ')
        if sepPort >= 0:
            host = aDestination[0:sepPort].strip()
            if sepPasswd > sepPort + 1:
                port = aDestination[sepPort+1:sepPasswd].strip()
                passwd = aDestination[sepPasswd:].strip()
            else:
                port = aDestination[sepPort+1:].strip()
                passwd = ""
        else:
            port = str(self.__defaultPort)
            if sepPasswd >= 0:
                host = aDestination[0:sepPasswd].strip()
                passwd = aDestination[sepPasswd:].strip()
            else:
                host = aDestination.strip()
                passwd = ""
        if not port.isdigit():
            self.logger.log(Logger.WARNING, "Bad value for port number "+repr(port)+" in "+aDestination+" destination")
            return
        alreadyAdded = False
        port = int(port)
        try:
            host = socket.gethostbyname(host)  # convert hostnames to IP addresses to avoid suffocating DNSs
        except socket.error as msg:
            self.logger.log(Logger.ERROR, "Error resolving "+host+": "+str(msg))
            return
        for h, p, dummyw in list(tempDestinations.keys()):
            if (h == host) and(p == port):
                alreadyAdded = True
                break
        destination = (host, port, passwd)
        if not alreadyAdded:
            self.logger.log(Logger.INFO, "Adding destination "+host+':'+repr(port)+' '+passwd)
            if destination in self.destinations:
                tempDestinations[destination] = self.destinations[destination]  # reuse previous options
            else:
                tempDestinations[destination] = copy.deepcopy(self.__defaultOptions)  # have a different set of options for each dest
            if destination not in self.destPrevData:
                self.destPrevData[destination] = {}	 # set it empty only if it's really new
            if destination not in self.senderRef:
                self.senderRef[destination] = copy.deepcopy(self.__defaultSenderRef)  # otherwise, don't reset this nr.
            if options != self.__defaultOptions:
                # we have to overwrite defaults with given options
                for key, value in list(options.items()):
                    self.logger.log(Logger.DEBUG, "Overwritting option: "+key+" = "+repr(value))
                    tempDestinations[destination][key] = value
        else:
            self.logger.log(Logger.NOTICE, "Destination "+host+":"+str(port)+" "+passwd+" already added. Skipping it")

    def __initializeFromFile(self, confFileName, tempDestinations):
        """
        Load destinations from confFileName file. If it's an URL(starts with "http://")
        load configuration from there. Put all destinations in tempDestinations hash.

        Calls addDestination for each line that doesn't start with # and
        has non-whitespace characters on it
        """
        try:
            if confFileName.find("http://") == 0:
                confFile = self.__getURL(confFileName)
                if confFile is None:
                    return
            else:
                confFile = open(confFileName)
        except IOError as ex:
            self.logger.log(Logger.ERROR, "Cannot open "+confFileName)
            self.logger.log(Logger.ERROR, "IOError: "+str(ex))
            return
        self.logger.log(Logger.INFO, "Adding destinations from "+confFileName)
        dests = []
        opts = {}
        while True:
            line = confFile.readline()
            if line == '':
                break
            line = line.strip()
            self.logger.log(Logger.DEBUG, "Reading line "+line)
            if len(line) == 0 or line[0] == '#':
                continue
            elif line.startswith("xApMon_"):
                m = re.match("xApMon_(.*)", line)
                if m is not None:
                    m = re.match(r"(\S+)\s*=\s*(\S+)", m.group(1))
                    if m is not None:
                        param = m.group(1)
                        value = m.group(2)
                        if value.upper() == "ON":
                            value = True
                        elif value.upper() == "OFF":
                            value = False
                        elif param.endswith("_interval"):
                            value = int(value)
                        if param == "loglevel":
                            self.logger.setLogLevel(value)
                        elif param == "maxMsgRate":
                            self.setMaxMsgRate(int(value))
                        elif param == "conf_recheck":
                            self.configRecheck = value
                        elif param == "recheck_interval":
                            self.configRecheckInterval = value
                        elif param.endswith("_data_sent"):
                            pass  # don't reset time in sys/job/general/_data_sent
                        else:
                            opts[param] = value
            else:
                dests.append(line)

        confFile.close()
        for line in dests:
            self.__addDestination(line, tempDestinations, opts)

    ###############################################################################################
    # Internal functions - Background monitor thread
    ###############################################################################################

    def __bgMonitor(self):
        """
        Background monitor thread
        """
        try:
            while not self.__bgMonitorEvent.isSet():
                self.__bgMonitorEvent.wait(10)
                if self.__bgMonitorEvent.isSet():
                    break
                if self.performBgMonitoring:
                    self.sendBgMonitoring()  # send only if the interval has elapsed
            self.__bgMonitorFinished.set()
        except:  # catch-all
            pass

    ###############################################################################################
    # Internal helper functions
    ###############################################################################################

    def __getURL(self, url, timeout=5):
        """
        this is a simplified replacement for urllib2 which doesn't support setting a timeout.
        by default, if timeout is not specified, it waits 5 seconds
        """
        r = re.compile(r"http://([^:/]+)(:(\d+))?(/.*)").match(url)
        if r is None:
            self.logger.log(Logger.ERROR, "Cannot open "+url+". Incorrectly formed URL.")
            return None
        host = r.group(1)
        if r.group(3) == None:
            port = 80  # no port is given, pick the default 80 for HTTP
        else:
            port = int(r.group(3))
        if r.group(4) == None:
            path = ""  # no path is give, let server decide
        else:
            path = r.group(4)
        sock = None
        err = None
        try:
            for res in socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
                af, socktype, proto, dummycanonname, sa = res
                try:
                    sock = socket.socket(af, socktype, proto)
                except socket.error as msg:
                    sock = None
                    err = msg
                    continue
                try:
                    if hasattr(sock, 'settimeout'):
                        self.logger.log(Logger.DEBUG, "Setting socket timeout with settimeout.")
                        sock.settimeout(timeout)
                    else:
                        self.logger.log(Logger.DEBUG, "Setting socket timeout with setsockopt.")
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, struct.pack("ii", timeout, 0))
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO, struct.pack("ii", timeout, 0))
                    sock.connect(sa)
                except socket.error as msg:
                    sock.close()
                    sock = None
                    err = msg
                    continue
                break
        except socket.error as msg:
            sock = None
            err = msg
        if sock is None:
            self.logger.log(Logger.ERROR, "Cannot open "+url)
            self.logger.log(Logger.ERROR, "SocketError: "+str(err))
            return None
        try:
            sock.send("GET "+path+" HTTP/1.0\n\n")
            data = ""
            done = False
            while not done:
                moreData = sock.recv(4096)
                data += moreData
                done = len(moreData) == 0
            sock.close()
            fd = io.StringIO(data)
            httpStatus = 0
            while True:
                line = fd.readline().strip()
                if line == "":
                    break  # exit at the end of file or at the first empty line(finish of http headers)
                r = re.compile(r"HTTP/\d.\d(\d+)").match(line)
                if r is not None:
                    httpStatus = int(r.group(1))
                if httpStatus == 200:
                    return fd
            else:
                self.logger.log(Logger.ERROR, "Cannot open "+url)
                if httpStatus == 401:
                    self.logger.log(Logger.ERROR, 'HTTPError: not authorized ['+str(httpStatus)+']')
                elif httpStatus == 404:
                    self.logger.log(Logger.ERROR, 'HTTPError: not found ['+str(httpStatus)+']')
                elif httpStatus == 503:
                    self.logger.log(Logger.ERROR, 'HTTPError: service unavailable ['+str(httpStatus)+']')
                else:
                    self.logger.log(Logger.ERROR, 'HTTPError: unknown error ['+str(httpStatus)+']')
                return None
        except socket.error as msg:
            self.logger.log(Logger.ERROR, "Cannot open "+url)
            self.logger.log(Logger.ERROR, "SocketError: "+str(msg))
            sock.close()
            return None

    def __directSendParams(self, destination, clusterName, nodeName, timeStamp, params):
        """ send parameters to destination url """

        if destination is None:
            self.logger.log(Logger.WARNING, "Destination is None")
            return

        host, port, passwd = destination
        crtSenderRef = self.senderRef[destination]

        hdrPacker = xdrlib.Packer()
        hdrPacker.pack_string("v:"+self.__version+"p:"+passwd)
        hdrPacker.pack_int(crtSenderRef['INSTANCE_ID'])
        hdrBuffer1 = hdrPacker.get_buffer()
        hdrPacker.reset()

        hdrPacker.pack_string(clusterName)
        hdrPacker.pack_string(nodeName)
        hdrBuffer2 = hdrPacker.get_buffer()
        hdrPacker.reset()

        hdrSize = len(hdrBuffer1) + len(hdrBuffer2) + 4

        paramPacker = xdrlib.Packer()
        paramBlocks = []

        crtParamsCount = 0
        crtParamsBuffer = ''
        crtParamsBuffSize = 0

        mapV = None
        if isinstance(params, dict):
            mapV = iter(list(params.items()))
        elif isinstance(params, list):
            mapV = params
        if mapV is not None:
            for name, value in mapV:
                if self.__packParameter(paramPacker, name, value):
                    buf = paramPacker.get_buffer()
                    bufLen = len(buf)
                    paramPacker.reset()
                    if bufLen + crtParamsBuffSize + hdrSize + 8 <= self.maxMsgSize:  # 8 for 2 ints: params count and result time
                        crtParamsBuffer += buf
                        crtParamsBuffSize += bufLen
                        crtParamsCount += 1
                    else:
                        self.logger.log(Logger.NOTICE, "Message is getting too big(Max size="+str(self.maxMsgSize)+"). Splitting it at "+name+"="+str(value))
                        paramBlocks.append((crtParamsCount, crtParamsBuffer))
                        paramPacker.reset()
                        crtParamsCount = 1
                        crtParamsBuffer = buf
                        crtParamsBuffSize = bufLen
        else:
            self.logger.log(Logger.ERROR, "Unsupported params type in sendParameters: " + str(type(params)))

        paramBlocks.append((crtParamsCount, crtParamsBuffer)) # update last params block
        paramPacker.reset()

        paramsTime = ''
        if timeStamp is not None and timeStamp > 0:
            paramPacker.pack_int(timeStamp)
            paramsTime = paramPacker.get_buffer()
            paramPacker.reset()

        for paramsCount, paramsBuffer in paramBlocks:
            if self.__shouldSend() == False:
                self.logger.log(Logger.WARNING, "Dropping packet since rate is too fast!")
                continue
            paramPacker.pack_int(paramsCount)
            crtSenderRef['SEQ_NR'] = (crtSenderRef['SEQ_NR'] + 1) % 2000000000  # wrap around 2 mld
            hdrPacker.pack_int(crtSenderRef['SEQ_NR'])
            bufferV = hdrBuffer1 + hdrPacker.get_buffer() + hdrBuffer2 + paramPacker.get_buffer() + paramsBuffer + paramsTime
            hdrPacker.reset()
            paramPacker.reset()
            bufLen = len(bufferV)
            self.logger.log(Logger.NOTICE, "Building XDR packet ["+str(clusterName)+"/"+str(nodeName)+"] <"+str(crtSenderRef['SEQ_NR'])+"/"+str(crtSenderRef['INSTANCE_ID'])+"> "+str(paramsCount)+" params, "+str(bufLen)+" bytes.")
            if bufLen > self.maxMsgSize:
                self.logger.log(Logger.WARNING, "Couldn't split parameter set(name/value pairs might be too large?). Message length is: "+str(bufLen)+". It might be dropped if > 1500. Sending anyway.")
            # send this buffer to the destination, using udp datagrams
            try:
                self.__udpSocket.sendto(bufferV, (host, port))
                self.logger.log(Logger.NOTICE, "Packet sent to "+host+":"+str(port)+" "+passwd)
            except socket.error as msg:
                self.logger.log(Logger.ERROR, "Cannot send packet to "+host+":"+str(port)+" "+passwd+": "+str(msg[1]))

    def __packParameter(self, xdrPacker, name, value):
        """ pack available parameters """
        if name is None or name is "":
            self.logger.log(Logger.WARNING, "Undefined parameter name. Ignoring value "+str(value))
            return False
        if value is None:
            self.logger.log(Logger.WARNING, "Ignore " + str(name)+ " parameter because of None value")
            return False
        if isinstance(name, unicode):
            name = str(name)
        if isinstance(value, unicode):
            value = str(value)
        try:
            typeValue = self.__valueTypes[type(value)]
            xdrPacker.pack_string(name)
            xdrPacker.pack_int(typeValue)
            self.__packFunctions[typeValue](xdrPacker, value)
            self.logger.log(Logger.NOTICE, "Adding parameter "+str(name)+" = "+str(value))
            return True
        except Exception as ex:
            self.logger.log(Logger.WARNING, "Error packing %s = %s; got %s" % (name, str(value), ex))
            return False

    # Destructor
    def __del__(self):
        if not self.__freed:
            self.free()


    def __shouldSend(self):
        """
        Decide if the current datagram should be sent.
        This decision is based on the number of messages previously sent
        """
        now = int(time.time())
        if now != self.__crtTime:
            # new time
            # update previous counters
            self.__prvSent = self.__hWeight * self.__prvSent + (1.0 - self.__hWeight) * self.__crtSent / (now - self.__crtTime)
            self.__prvTime = self.__crtTime
            self.logger.log(Logger.DEBUG, "previously sent: " + str(self.__crtSent) + "; dropped: " + str(self.__crtDrop))
            # reset current counter
            self.__crtTime = now
            self.__crtSent = 0
            self.__crtDrop = 0

        # compute the history
        valSent = self.__prvSent * self.__hWeight + self.__crtSent * (1 - self.__hWeight)

        doSend = True

        # when we should start dropping messages
        level = self.maxMsgRate - float(self.maxMsgRate // 10)

        if valSent > (self.maxMsgRate - level):
            if random.randint(0, int(self.maxMsgRate // 10)) >= (self.maxMsgRate - valSent):
                doSend = False

        # counting sent and dropped messages
        if doSend:
            self.__crtSent += 1
        else:
            self.__crtDrop += 1

        return doSend

    @staticmethod
    def __getInstanceID():
        """
        Try to generate a more random instance id. It takes the process ID and
        combines it with the last digit from the IP addess and a random number
        
        Notice the random number generated must be represented by a signed int
        of 32 bits, otherwise it breaks xdr library.
        """
        pid = os.getpid()
        try:
            sip = socket.gethostbyname(socket.gethostname())
            ip = int(sip[1+sip.rfind('.'):])
        except socket.error:
            ip = random.randint(0, 255)
        rnd = random.randint(0, 255)
        return ((pid << 8) | (ip << 16) | rnd) & 2**31 - 1


    ################################################################################################
    # Private variables. Don't touch
    ################################################################################################

    __valueTypes = {
        type("string"): 0,  # XDR_STRING(see ApMon.h from C/C++ ApMon version)
        type(1): 2, 		# XDR_INT32
        LongType: 5,		# send longs as doubles (Problem with Python 3 since long is same as int)
        type(1.0): 5}		# XDR_REAL64

    __packFunctions = {
        0: xdrlib.Packer.pack_string,
        2: xdrlib.Packer.pack_int,
        5: xdrlib.Packer.pack_double}

    __defaultPort = 8884
    __version = "2.2.20-py"			# apMon version number

