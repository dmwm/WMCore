#!/usr/bin/env python
"""
_PerfUtils_

Utilities for getting/manipulating details for a PerformanceReport

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: PerfUtils.py,v 1.1 2008/10/08 15:34:15 fvlingen Exp $"
__author__ = "evansde@fnal.gov"


import os

def readCPUInfo():
    """
    _readCPUInfo_

    Read CPUInfo file if exists and return a list
    of the cpus on the node

    """
    if not os.path.exists("/proc/cpuinfo"):
        return []

    try:
        handle = open("/proc/cpuinfo", "r")
        content = handle.readlines()
        handle.close()
    except Exception,ex:
        return []
    result = []
    currentCPU = None
    for line in content:
        if line.startswith("processor"):
            if currentCPU != None:
                result.append(currentCPU)
                
            currentCPU = {}
            coreNum = line.split(":")[1].strip()
            currentCPU['Core'] = coreNum
            
        if line.startswith("cpu MHz"):
            cpuVal = line.split(":")[1].strip()
            currentCPU['MHz'] = cpuVal

        if line.startswith("model name"):
            model = line.split(":",1)[1].strip()
            currentCPU['Model'] = model
    
    if currentCPU != None:
        result.append(currentCPU)
    
    return result


def readMeminfo():
    """
    _readMeminfo_

    Grab details about the machines memory from /proc/meminfo

    """
    result = {
        "MemTotal" : None, 
        }

    if not os.path.exists("/proc/meminfo"):
        return result

    try:
        handle = open("/proc/meminfo", "r")
        content = handle.readlines()
        handle.close()
    except Exception,ex:
        return result


    for metric in result.keys():
        for line in content:
            if line.startswith(metric):
                value = line.split(":", 1)[1]
                value = value.replace("kB", "")
                value = value.strip()
                result[metric] = value
    return result

