#!/usr/bin/env python
"""
_PerfInfo

Performance Report object that can be embedded in a job report


Eventually should encompass:

These may be on a per module and/or overall summary basis
timing
 memory
 trigger
 message summary
 and possibly storage IO if I can work out what the TFile adaptor actually does

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: PerfInfo.py,v 1.1 2008/10/08 15:34:15 fvlingen Exp $"
__author__ = "evansde@fnal.gov"


from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery

import WMCore.FwkJobReport.PerfUtils as PerfUtils



class PerfInfo:
    """
    _PerfInfo_

    Standardized object for storing/parsing/persisting
    performance reports as part of a job report

    """
    def __init__(self):
        self.cpus = {}
        self.memory = {}
        self.summaries = {}
        self.modules = {}



    def addCPU(self, core, speed, desc):
        """
        _addCPU_

        Add a record of a CPU on the node where the job was run

        """
        self.cpus[core] = {
            "Speed" : speed,
            "Description" : desc,
            }
        return

    def getInfoOnWorker(self):
        """
        _getInfoOnWorker_

        Get the CPU and memory Info from /proc
        Only do this on the WN

        """
        self.memory = PerfUtils.readMeminfo()
        cpus = PerfUtils.readCPUInfo()
        [ self.addCPU(x['Core'], x['MHz'], x['Model']) for x in cpus ]
        return

    def addSummary(self, metricClass, **metrics):
        """
        _addSummary_

        Add a performance summary for the metric class name
        provided.

        Duplicate metric keys will be updated

        """
        if not self.summaries.has_key(metricClass):
            self.summaries[metricClass] = {}

        self.summaries[metricClass].update(metrics)
        return

    def addModule(self, metricClass, moduleName, **metrics):
        """
        _addModule_

        Add a module report for a given metric class

        """
        if not self.modules.has_key(moduleName):
            self.modules[moduleName] = {}

        if not self.modules[moduleName].has_key(metricClass):
            self.modules[moduleName][metricClass] = []

        self.modules[moduleName][metricClass].append(metrics)
        return



    def save(self):
        """
        _save_

        Save as XML via IMProvNode
        returns IMProvNode
        """
        result = IMProvNode("PerformanceReport")

        cpu = IMProvNode("CPU")
        result.addNode(cpu)
        for cpuName, cpuInfo in self.cpus.items():
            cpuNode = IMProvNode("CPUCore", None, Core = cpuName)
            [ cpuNode.addNode(IMProvNode("Property", y, Name = x))
              for x,y in cpuInfo.items() ]
            cpu.addNode(cpuNode)

        mem = IMProvNode("Memory")
        result.addNode(mem)
        [ mem.addNode(IMProvNode("Property", y, Name = x))
          for x,y in self.memory.items() ]


        for metName, metSumm in self.summaries.items():
            summaryNode = IMProvNode("PerformanceSummary",
                                     None, Metric = metName)

            [ summaryNode.addNode(
                IMProvNode("Metric", None, Name = x, Value = y) )
              for x,y in metSumm.items()
              ]
            result.addNode(summaryNode)

        for modName in self.modules.keys():
            for metricName, metrics in self.modules[modName].items():
                modNode = IMProvNode("PerformanceModule", None,
                                     Metric = metricName,  Module= modName)

                [
                    [ modNode.addNode(
                    IMProvNode("Metric", None, Name = x, Value = y))
                      for x,y in m.items() ]
                    for m in metrics
                    ]

                result.addNode(modNode)

        return result

    def load(self, improvNode):
        """
        _load_

        Extract Performance reports from the improvNode provided
        Uses a Relative path query

        """
        cpuQ = IMProvQuery("PerformanceReport/CPU/CPUCore")
        memQ = IMProvQuery("PerformanceReport/Memory/Property")
        summaryQ = IMProvQuery("PerformanceReport/PerformanceSummary")
        moduleQ  = IMProvQuery("PerformanceReport/PerformanceModule")

        for cpu in cpuQ(improvNode):
            core = cpu.attrs.get("Core", None)
            if core == None:
                continue
            core = str(core)
            speed = None
            desc = None
            for child in cpu.children:
                if child.attrs.get('Name', None) == "Description":
                    desc = str(child.chardata)
                if child.attrs.get('Name', None) == 'Speed':
                    speed = str(child.chardata)
            self.addCPU(core, speed, desc)

        for mem in memQ(improvNode):
            name = mem.attrs.get("Name", None)
            if name == None: continue
            self.memory[str(name)] = str(mem.chardata)



        for summNode in summaryQ(improvNode):
            metricName = summNode.attrs.get("Metric", None)
            if metricName == None: continue
            metrics = {}
            for metric in IMProvQuery("Metric")(summNode):
                metName = metric.attrs.get("Name", None)
                metVal = metric.attrs.get("Value", None)
                if metName == None: continue
                metrics[str(metName)] = str(metVal)

            self.addSummary(str(metricName), **metrics)

        for modNode in moduleQ(improvNode):
            metricName = modNode.attrs.get("Metric", None)
            modName = modNode.attrs.get("Module", None)
            if metricName == None: continue
            if modName == None: continue
            metrics = {}
            for metric in IMProvQuery("Metric")(modNode):
                metName = metric.attrs.get("Name", None)
                metVal = metric.attrs.get("Value", None)
                if metName == None: continue
                metrics[str(metName)] = str(metVal)
            self.addModule(str(metricName), modName, **metrics)

        return


