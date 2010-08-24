#!/usr/bin/env python
"""
_StorageInfo_

Interim converter to turn the storage stats into a perf report

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: StorageInfo.py,v 1.1 2008/10/08 15:34:16 fvlingen Exp $"
__author__ = "evansde@fnal.gov"



from IMProv.IMProvQuery import IMProvQuery

def handleStorageStats(improvNode, reportRef):
    """
    _handleStorageStats_

    Look for the detailed storage stats information and
    collapse it to a set of performance report instances

    """
    #  //
    # // Backwards compatibility
    #//
    if len(improvNode.chardata.strip()) > 0:
        reportRef.storageStatistics = str(improvNode.chardata.strip())
        return

    #  //
    # // new format report
    #//
    base = "/StorageStatistics/storage-factory-summary/"
    statsBase = "%sstorage-factory-stats" % base
    statsQ  = IMProvQuery(
        "%s/storage-timing-summary/counter-value" % statsBase)

    rootQ = IMProvQuery(
        "%s/storage-root-summary/counter-value" % statsBase)

    paramBase = "%sstorage-factory-params/param" % base
    paramQ = IMProvQuery(paramBase)


    allStats = {}
    for stats in statsQ(improvNode):
        subsystem = stats.attrs.get("subsystem", None)
        if subsystem == None:
            continue
        subsystem = str(subsystem)
        counterName = stats.attrs.get("counter-name", None)
        if counterName == None:
            continue
        counterName = str(counterName)

        keyName = "%s-%s" % (subsystem, counterName)
        for statKey, statVal in stats.attrs.items():
            statKey = str(statKey)
            statVal = str(statVal)
            if statKey in ("subsystem", "counter-name"):
                continue
            dataKey = "%s-%s" % (keyName, statKey)
            allStats[dataKey] = statVal


    #print allStats
    allRoots = {}
    for root in rootQ(improvNode):
        subsystem = root.attrs.get("subsystem", None)
        if subsystem == None:
            continue
        subsystem = str(subsystem)
        counterName = root.attrs.get("counter-name", None)
        if counterName == None:
            continue
        counterName = str(counterName)

        keyName = "%s-%s" % (subsystem, counterName)
        for rootKey, rootVal in root.attrs.items():
            rootKey = str(rootKey)
            rootVal = str(rootVal)
            if rootKey in ("subsystem", "counter-name"):
                continue
            dataKey = "%s-%s" % (keyName, rootKey)
            allRoots[dataKey] = rootVal


    allParams = {}
    for param in paramQ(improvNode):
        paramName = param.attrs.get("name", None)
        if paramName == None:
            continue
        paramName = str(paramName)
        paramVal = param.attrs.get("value", None)
        if paramVal == None:
            continue
        allParams[paramName] = str(paramVal)


    reportRef.performance.addSummary("StorageTiming", **allStats)
    reportRef.performance.addSummary("StorageRoot", **allRoots)
    reportRef.performance.addSummary("StorageParams", **allParams)
    return
