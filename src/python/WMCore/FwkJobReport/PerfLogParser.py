#!/usr/bin/env python
"""
_LogParser_

Interim tool to extract performance information from stdout
until it can be completely rolled into CMSSW


"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: PerfLogParser.py,v 1.1 2008/10/08 15:34:15 fvlingen Exp $"
__author__ = "evansde@fnal.gov"

#FXME: needs to be rolled into CMSSW

import os
import re


CPUEventSearch = re.compile("CPU/event = [0-9\.]+")
RealEventSearch = re.compile("Real/event = [0-9\.]+")
TrigTotalSearch = re.compile("total = [0-9]+")
TrigPassedSearch = re.compile("passed = [0-9]+")
TrigFailedSearch = re.compile("failed = [0-9]+")



def readReports(prefix, filename):
    """
    _readReports_

    Returns a list of lines from the file
    provided for further processing that start with the prefix

    """
    if not os.path.exists(filename):
        return []

    result = []
    handle = open(filename, 'r')
    for line in handle:
        if line.startswith(prefix):
            result.append(line.replace(prefix, "").strip())


    return result



class TimeReportMaker:
    """
    _TimeReportMaker_

    Generate a time report from a stderr file

    """
    def __init__(self, filename):
        self.content = readReports("TimeReport", filename)
        self.verbose = False

    def __call__(self, perfRep):
        """
        _operator()_

        Generate a time report and insert it into the PerformanceReport
        instance provided

        """

        perfRep.addSummary("Timing", **self.eventSummary())        

        if self.verbose == False:
            return

        for modName, modMetrics in self.moduleSummary().items():
            perfRep.addModule("Timing", modName, **modMetrics)

        for pathName, pathMetrics in self.pathSummary().items():
            perfRep.addModule("Timing", pathName, **pathMetrics)

        for pathName, pathMetrics in self.endpathSummary().items():
            perfRep.addModule("Timing", pathName, **pathMetrics)
            

    def extractContent(self, start, end):
        """
        _extractContent_

        Extract a subset of the content starting with the line
        start and continuing until end

        """
        localContent = []
        inContent = False
        for line in self.content:
            if line.startswith(start):
                inContent = True
                continue
                
            if inContent == True:
                if line.startswith(end):
                    break
                else:
                    localContent.append(line)

        return localContent


    def eventSummary(self):
        """
        _eventSummary_

        """
        start =  "---------- Event  Summary ---[sec]----"
        localContent = self.extractContent(start, "----")
        cpuPerEvent = None
        realPerEvent = None
        for line in localContent:
            cpuPerEv = CPUEventSearch.findall(line)
            if len(cpuPerEv) > 0:
                cpuPerEvent = cpuPerEv[0]
            realPerEv =  RealEventSearch.findall(line)
            if len(realPerEv) > 0:
                realPerEvent =  realPerEv[0]

        result = {}
        if realPerEvent != None:
            value = realPerEvent.split("=")[1]
            result['RealPerEvent'] = value.strip()

        if cpuPerEvent != None:
            value = cpuPerEvent.split("=")[1]
            result['CPUPerEvent'] = value.strip()

        return result
            
        

    def moduleSummary(self):
        """
        _moduleSummary_

        Extract the module timing summary
        """
        
        start = "---------- Module Summary ---[sec]----"
        localContent = self.extractContent(start, "----")
        result = {}

        for data in localContent:
            elems = data.split()
            if len(elems) != 7: continue
            modName = elems[6]
            if modName == "Name": continue
            perEventCPU = elems[0]
            perEventReal = elems[1]
            perModRunCPU = elems[2]
            perModReal = elems[3]
            perModVisitCPU = elems[4]
            perModVisitReal = elems[5] 


            result[modName] = {
                "PerEventCPU" : elems[0],
                "PerEventReal" : elems[1],
                "PerModuleRunCPU": elems[2],
                "PerModuleRunReal" : elems[3],
                "PerModuleVisitCPU" : elems[4],
                "PerModuleVisitReal" : elems[5]
                }

        return result
            
    def pathSummary(self):
        """
        _pathSummary_

        """
        start = "---------- Path   Summary ---[sec]----"
        localContent = self.extractContent(start, "----")
        result = {}
        for line in localContent:
            elems = line.split()
            if len(elems) != 5: continue
            pathName = elems[4]
            if pathName == "Name": continue
            
            result[pathName] = {
                "PerEventCPU" : elems[0],
                "PerEventReal" : elems[1],
                "PerPathRunCPU": elems[2],
                "PerPathRunReal" : elems[3],
                }
        return result


    def endpathSummary(self):
        """
        _endpathSummary_

        """
        start = "-------End-Path   Summary ---[sec]----"
        
        localContent = self.extractContent(start, "----")
        result = {}
        for line in localContent:
            elems = line.split()
            if len(elems) != 5: continue
            pathName = elems[4]
            if pathName == "Name": continue

            result[pathName] = {
                "PerEventCPU" : elems[0],
                "PerEventReal" : elems[1],
                "PerPathRunCPU": elems[2],
                "PerPathRunReal" : elems[3],
                }
        return result
            
class TrigReportMaker:
    """
    _TrigReportMaker_

    Generate a trigger report from a stderr file

    """
    def __init__(self, filename):
        self.content = readReports("TrigReport", filename)
        self.verbose = False
        
    def extractContent(self, start, end):
        """
        _extractContent_

        Extract a subset of the content starting with the line
        start and continuing until end

        """
        localContent = []
        inContent = False
        for line in self.content:
            if line.startswith(start):
                inContent = True
                continue
                
            if inContent == True:
                if line.startswith(end):
                    break
                else:
                    localContent.append(line)

        return localContent



    def __call__(self, perfRep):
        """
        _operator(PerformanceReport)_

        Add Trigger Report information to the PerformanceReport

        """
        perfRep.addSummary("TrigReport", **self.eventSummary())        

        if self.verbose == False:
            return
        for modName, modMetrics in self.moduleSummary().items():
            perfRep.addModule("TrigReport", modName, **modMetrics)
        
        return

    def eventSummary(self):
        """
        _eventSummary_

        """
        start = "---------- Event  Summary ------------"
        localContent = self.extractContent(start, "----")
        result = {}

        total = None
        passed = None
        failed = None
        for line in localContent:
            totalS = TrigTotalSearch.findall(line)
            if len(totalS) > 0:
                total = totalS[0]
            passedS =  TrigPassedSearch.findall(line)
            if len(passedS) > 0:
                passed = passedS[0]

            failedS = TrigFailedSearch.findall(line)
            if len(failedS) > 0:
                failed = failedS[0]

        result = {}
        if total != None:
            result['TotalEvents'] = total.split("=")[1].strip()
        if passed != None:
            result['PassedEvents'] = passed.split("=")[1].strip()
        if failed != None:
            result['FailedEvents'] = failed.split("=")[1].strip()
            
        
        return result

    def moduleSummary(self):
        """
        _moduleSummary_

        """
        
        start = "---------- Module Summary ------------"
        localContent = self.extractContent(start, "----")
        result = {}

        for line in localContent:
            elems = line.split()
            if len(elems) != 6: continue
            modName = elems[5]
            if modName == "Name": continue
            result[modName] = {
                "Visited" : elems[0],
                "Run"     : elems[1],
                "Passed"  : elems[2],
                "Failed"  : elems[3],
                "Error"   : elems[4],
                }
        return result

def average(numbers):
    return sum(numbers) / len(numbers)   

class PerfReportLogReportMaker:
    """
    _PerfReportLogReportMaker_

    Read the output of the PerfReport.log file to generate
    some CPU and memory usage metrics for the job
    
    """
    def __init__(self, logfilePath):
        self.content = readReports("", logfilePath)
        self.verbose = False
        self.rss   =  []
        self.vsize =  []
        self.pcpu  =  []
        self.pmem  =  []
        

    def __call__(self, perfRep):
        """
        _operator(PerformanceReport)_

        Add details to the performance report provided

        """
        for line in self.content:
            elems = line.split()
            if len(elems) < 7: continue
            
            self.rss.append(int(elems[2]))
            self.vsize.append(int(elems[3]))
            self.pcpu.append(float(elems[4]))
            self.pmem.append(float(elems[5]))

            
        if len(self.pcpu) > 0:
            perfRep.addSummary("PercentCPU",
                               MinPercentCPU = min(self.pcpu),
                               MaxPercentCPU = max(self.pcpu),
                               AvgPercentCPU = average(self.pcpu))
        if len(self.pmem) > 0:
            perfRep.addSummary("PhysicalMemory",
                               MinPhysicalMemory = min(self.pmem),
                               MaxPhysicalMemory = max(self.pmem),
                               AvgPhysicalMemory = average(self.pmem))
            
        if len(self.rss) > 0:
            perfRep.addSummary("RSSMemory",
                               MinRSSMemory = min(self.rss),
                               MaxRSSMemory = max(self.rss),
                               AvgRSSMemory = average(self.rss))
        if len(self.vsize) > 0:
            perfRep.addSummary("VSizeMemory",
                               MinVSizeMemory = min(self.vsize),
                               MaxVSizeMemory = max(self.vsize),
                               AvgVSizeMemory = average(self.vsize))
            
                           
        return
        


def makePerfReports(perfRep, stderr = None, perfLog = None , verbose = False):
    """
    _makePerfReports_

    Util to generate a perf report
    """

    if stderr != None:
        try:
            timeRep = TimeReportMaker(stderr)
            timeRep.verbose = verbose
            trigRep = TrigReportMaker(stderr)
            trigRep.verbose = verbose
            timeRep(perfRep)
            trigRep(perfRep)

        except Exception, ex:
            msg = "Error Creating Performance Reports from stderr file:\n"
            msg += "%s\n" % stderr
            msg += str(ex)
            print msg


    if perfLog != None:
        try:
            perfLogMaker = PerfReportLogReportMaker(perfLog)
            perfLogMaker.verbose = verbose
            perfLogMaker(perfRep)
        except Exception, ex:
            msg = "Error creating performance reports from PerfReport.log\n"
            msg += str(ex)
            print msg

    return
