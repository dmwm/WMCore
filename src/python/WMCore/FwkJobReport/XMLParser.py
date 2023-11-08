#!/usr/bin/env python
"""
_XMLParser_

Read the raw XML output from the cmsRun executable.
"""
from __future__ import division, print_function

import logging
import re

from WMCore.Algorithms.ParseXMLFile import coroutine, xmlFileToNode
from WMCore.DataStructs.Run import Run
from WMCore.FwkJobReport import Report


pat_int = re.compile(r'(^[0-9-]$|^[0-9-][0-9]*$)')
pat_float = re.compile(r'(^[-]?\d+\.\d*$|^\d*\.{1,1}\d+$)')


def reportBuilder(nodeStruct, report, target):
    """
    _reportBuilder_

    Driver for coroutine pipe for building reports from the Node
    structure.
    """
    for node in nodeStruct.children:
        target.send((report, node))


@coroutine
def reportDispatcher(targets):
    """
    _reportDispatcher_

    Top level routine for dispatching the parts of the job report to the
    handlers.
    """
    while True:
        report, node = (yield)
        if node.name != "FrameworkJobReport":
            print("Not Handling: ", node.name)
            # TODO: throw
            continue

        for subnode in node.children:
            if subnode.name == "File":
                targets['File'].send((report, subnode))
            elif subnode.name == "InputFile":
                targets['InputFile'].send((report, subnode))
            elif subnode.name == "AnalysisFile":
                targets['AnalysisFile'].send((report, subnode))
            elif subnode.name == "PerformanceReport":
                targets['PerformanceReport'].send((report, subnode))
            elif subnode.name == "FrameworkError":
                targets['FrameworkError'].send((report, subnode))
            elif subnode.name == "SkippedFile":
                targets['SkippedFile'].send((report, subnode))
            elif subnode.name == "FallbackAttempt":
                targets['FallbackAttempt'].send((report, subnode))
            elif subnode.name == "SkippedEvent":
                targets['SkippedEvent'].send((report, subnode))
            else:
                setattr(report.report.parameters, subnode.name, subnode.text)


@coroutine
def fileHandler(targets):
    """
    _fileHandler_

    coroutine to create files and handle sub data in the appropriate
    dispatchers

    """
    while True:
        report, node = (yield)
        moduleName = None
        moduleNode = [x for x in node.children if x.name == "ModuleLabel"][0]
        moduleName = moduleNode.text

        fileRef = report.addOutputFile(moduleName)
        fileAttrs = {}
        for subnode in node.children:
            if subnode.name == "Inputs":
                targets['Inputs'].send((fileRef, subnode))
            elif subnode.name == "Runs":
                targets['Runs'].send((fileRef, subnode))
            elif subnode.name == "Branches":
                targets['Branches'].send((fileRef, subnode))
            else:
                fileAttrs[subnode.name] = subnode.text

        Report.addAttributesToFile(fileRef, lfn=fileAttrs["LFN"],
                                   pfn=fileAttrs["PFN"], catalog=fileAttrs["Catalog"],
                                   module_label=fileAttrs["ModuleLabel"],
                                   guid=fileAttrs["GUID"],
                                   output_module_class=fileAttrs["OutputModuleClass"],
                                   events=int(fileAttrs["TotalEvents"]),
                                   branch_hash=fileAttrs["BranchHash"])


@coroutine
def inputFileHandler(targets):
    """
    _inputFileHandler_

    coroutine to create input files in the report and dispatch
    sub data down the pipeline

    """
    while True:
        report, node = (yield)
        moduleName = None
        moduleNode = [x for x in node.children if x.name == "ModuleLabel"][0]
        moduleName = moduleNode.text

        fileRef = report.addInputFile(moduleName)
        fileAttrs = {}
        for subnode in node.children:
            if subnode.name == "Runs":
                targets['Runs'].send((fileRef, subnode))
            elif subnode.name == "Branches":
                targets['Branches'].send((fileRef, subnode))
            else:
                fileAttrs[subnode.name] = subnode.text

        Report.addAttributesToFile(fileRef, lfn=fileAttrs["LFN"],
                                   pfn=fileAttrs["PFN"], catalog=fileAttrs["Catalog"],
                                   module_label=fileAttrs["ModuleLabel"],
                                   guid=fileAttrs["GUID"], input_type=fileAttrs["InputType"],
                                   input_source_class=fileAttrs["InputSourceClass"],
                                   events=int(fileAttrs["EventsRead"]))


@coroutine
def analysisFileHandler(targets):
    """
    _analysisFileHandler_

    handle analysis file entries in the report

    """
    while True:
        report, node = (yield)
        filename = None
        attrs = {}
        for subnode in node.children:
            if subnode.name == "FileName":
                filename = subnode.text
            else:
                attrs[subnode.name] = subnode.attrs.get('Value', None)

        report.addAnalysisFile(filename, **attrs)


@coroutine
def errorHandler():
    """
    _errorHandler_

    Handle FrameworkError reports.
    """
    while True:
        report, node = (yield)
        excepcode = node.attrs.get("ExitStatus", 8001)
        exceptype = node.attrs.get("Type", "CMSException")

        # There should be atmost one step in the report at this point in time.
        if len(report.listSteps()) == 0:
            report.addError("unknownStep", excepcode, exceptype, node.text)
        else:
            report.addError(report.listSteps()[0], excepcode, exceptype, node.text)


@coroutine
def skippedFileHandler():
    while True:
        report, node = (yield)
        lfn = node.attrs.get("Lfn", None)
        pfn = node.attrs.get("Pfn", None)
        report.addSkippedFile(lfn, pfn)


@coroutine
def fallbackAttemptHandler():
    while True:
        report, node = (yield)
        lfn = node.attrs.get("Lfn", None)
        pfn = node.attrs.get("Pfn", None)
        report.addFallbackFile(lfn, pfn)


@coroutine
def skippedEventHandler():
    while True:
        report, node = (yield)
        run = node.attrs.get("Run", None)
        event = node.attrs.get("Event", None)
        if run is None:
            continue
        if event is None:
            continue
        report.addSkippedEvent(run, event)


@coroutine
def runHandler():
    """
    _runHandler_

    Sink to add run information to a file.  Given the following XML:
      <Runs>
      <Run ID="122023">
        <LumiSection NEvents="100" ID="215"/>
        <LumiSection NEvents="100" ID="216"/>
      </Run>
      <Run ID="122024">
        <LumiSection ID="1"/>
        <LumiSection ID="2"/>
      </Run>
      </Runs>

    Create a WMCore.DataStructs.Run object for each run and call the
    addRunInfoToFile() function to add the run information to the file
    section.
    """
    while True:
        fileSection, node = (yield)
        for subnode in node.children:
            if subnode.name == "Run":
                runId = subnode.attrs.get("ID", None)
                if runId is None:
                    continue

                lumis = []
                for lumi in subnode.children:
                    if "ID" in lumi.attrs:
                        lumiNumber = int(lumi.attrs['ID'])
                        nEvents = lumi.attrs.get("NEvents", None)
                        if nEvents is not None:
                            try:
                                nEvents = int(nEvents)
                            except ValueError:
                                nEvents = None
                        lumis.append((lumiNumber, nEvents))
                runInfo = Run(runNumber=runId)
                runInfo.extendLumis(lumis)

                Report.addRunInfoToFile(fileSection, runInfo)

@coroutine
def branchHandler():
    """
    _branchHandler_

    Sink to pack branch information into a file.  Given the following XML:
      <Branches>
        <Branch>Branch Name 1</Branch>
        <Branch>Branch Name 2</Branch>
      </Branches>

    Create a list containing all the branch names as use the
    addBranchNamesToFile method to add them to the fileSection.

    Nulled out, we dont need these anyways...

    """
    while True:
        fileSection, node = (yield)
        pass
        # branches = [ subnode.text for subnode in node.children
        #             if subnode.name == "Branch" ]
        # Report.addBranchNamesToFile(fileSection, branches)


@coroutine
def inputAssocHandler():
    """
    _inputAssocHandler_

    Sink to handle output:input association information.  Given the following
    XML:
      <Input>
        <LFN>/path/to/some/lfn.root</LFN>
        <PFN>/some/pfn/info/path/to/some/lfn.root</PFN>
      </Input>

    Extract the LFN and call the addInputToFile() function to associate input to
    output in the FWJR.
    """
    while True:
        fileSection, node = (yield)
        for inputnode in node.children:
            data = {}
            for subnode in inputnode.children:
                data.__setitem__(subnode.name, subnode.text)
            Report.addInputToFile(fileSection, data["LFN"], data['PFN'])


@coroutine
def perfRepHandler(targets):
    """
    _perfRepHandler_

    handle performance report subsections

    """
    while True:
        report, node = (yield)
        perfRep = report.report.performance
        perfRep.section_("summaries")
        perfRep.section_("cpu")
        perfRep.section_("memory")
        perfRep.section_("storage")
        perfRep.section_("cmssw")
        for subnode in node.children:
            metric = subnode.attrs.get('Metric', None)
            targets['PerformanceSummary'].send((perfRep.cmssw, subnode))
            if metric == "Timing":
                targets['CPU'].send((perfRep.cpu, subnode))
            elif metric == "SystemMemory" or metric == "ApplicationMemory":
                targets['Memory'].send((perfRep.memory, subnode))
            elif metric == "StorageStatistics":
                targets['Storage'].send((perfRep.storage, subnode))
            else:
                targets['PerformanceSummary'].send((perfRep.summaries,
                                                    subnode))

@coroutine
def perfSummaryHandler():
    """
    _perfSummaryHandler_

    Sink to handle performance summaries

    """
    while True:
        report, node = (yield)
        summary = node.attrs.get('Metric', None)
        module = node.attrs.get('Module', None)
        if summary is None:
            continue
        site = None
        if module == 'XrdSiteStatistics':
            site = summary
            summary = 'XrdSiteStatistics'

        # Add performance section if it doesn't exist
        if not hasattr(report, summary):
            report.section_(summary)
        summRep = getattr(report, summary)

        for subnode in node.children:
#             setattr(summRep, subnode.attrs['Name'],
#                     subnode.attrs['Value'])
            name = subnode.attrs['Name']
            value = subnode.attrs['Value']
            if module == 'XrdSiteStatistics':
                value = castXrdSiteStatistics(summRep, name, site, value)
            else:
                value = castMetricValue(value)
            setattr(summRep, name, value)


def castMetricValue(value):
    """
    Perform casting of input value to proper data-type expected in MONIT
    :param value: input value, can be in string or actual data type form, e.g. "1" vs 1
    :return: value of proper data-type based on regexp pattern matching
    """
    if isinstance(value, str):
        # strip off leading and trailing spaces from string values to allow proper data-type casting
        value = value.lstrip().rstrip()
    if value == 'false':
        value = False
    elif value == 'true':
        value = True
    elif pat_float.match(value):
        value = float(value)
    elif pat_int.match(value):
        value = int(value)
    return value


def castXrdSiteStatistics(summRep, name, site, value):
    """
    Cast XrdSiteStatistics value from given summary report and name of the metric.

    This is special case to be used for CMSSW XML report with the following performance module section

    <PerformanceReport>
      <PerformanceModule Metric="cern.ch"  Module="XrdSiteStatistics" >
        <Metric Name="read-numOperations" Value="0"/>
        ...
      </PerformanceModule>
    </PerformanceReport>

    as it does not satisfies static schema and we should treat it separately

    :param summRep: summary performance object
    :param name: name of metric
    :param site: name of the CMS site
    :param value: value for given site
    :return: list of site values in the form of the dictionary, e.g.
    [{"site": "cern.ch", "value": 1}, {"site": "infn.it", "value": 2}]
    """
    cdict = summRep.dictionary_()
    eValue = cdict.get(name, [])
    vdict = {"site": site, "value": value}
    eValue.append(vdict)
    value = eValue
    return value

@coroutine
def perfCPUHandler():
    """
    _perfCPUHandler_

    sink that packs CPU reports into the job report

    """
    while True:
        report, node = (yield)
        for subnode in node.children:
            setattr(report, subnode.attrs['Name'], subnode.attrs['Value'])


@coroutine
def perfMemHandler():
    """
    _perfMemHandler_

    Pack memory performance reports into the report
    """
    # Make a list of performance info we actually want
    goodStatistics = ['PeakValueRss', 'PeakValueVsize', 'LargestRssEvent-h-PSS']

    while True:
        report, node = (yield)
        for prop in node.children:
            if prop.attrs['Name'] in goodStatistics:
                if prop.attrs['Name'] == 'LargestRssEvent-h-PSS':
                    # need to remove - chars from name as it buggers up downtstream code
                    setattr(report, 'PeakValuePss', prop.attrs['Value'])
                else:
                    setattr(report, prop.attrs['Name'], prop.attrs['Value'])


def checkRegEx(regexp, candidate):
    if re.compile(regexp).match(candidate) == None:
        return False
    return True


@coroutine
def perfStoreHandler():
    """
    _perfStoreHandler_

    Handle the information from the Storage report
    """

    # Make a list of performance info we actually want
    goodStatistics = ['Timing-([a-z]{4})-read(v?)-totalMegabytes',
                      'Timing-([a-z]{4})-write(v?)-totalMegabytes',
                      'Timing-([a-z]{4})-read(v?)-totalMsecs',
                      'Timing-([a-z]{4})-read(v?)-numOperations',
                      'Timing-([a-z]{4})-write(v?)-numOperations',
                      'Timing-([a-z]{4})-read(v?)-maxMsecs',
                      'Timing-tstoragefile-readActual-numOperations',
                      'Timing-tstoragefile-read-numOperations',
                      'Timing-tstoragefile-readViaCache-numSuccessfulOperations',
                      'Timing-tstoragefile-read-numOperations',
                      'Timing-tstoragefile-read-totalMsecs',
                      'Timing-tstoragefile-write-totalMsecs',
                      ]

    while True:
        report, node = (yield)
        logging.debug("Preparing to parse storage statistics")
        storageValues = {}
        for prop in node.children:
            name = prop.attrs['Name']
            for statName in goodStatistics:
                if checkRegEx(statName, name):
                    storageValues[name] = float(prop.attrs['Value'])
                    # setattr(report, name, prop.attrs['Value'])

        writeMethod = None
        readMethod = None
        # Figure out read method
        for key in storageValues:
            if checkRegEx('Timing-([a-z]{4})-read(v?)-numOperations', key):
                if storageValues[key] != 0.0:
                    # This is the reader
                    readMethod = key.split('-')[1]
                    break
        # Figure out the write method
        for key in storageValues:
            if checkRegEx('Timing-([a-z]{4})-write(v?)-numOperations', key):
                if storageValues[key] != 0.0:
                    # This is the reader
                    writeMethod = key.split('-')[1]
                    break

        # Then assemble the information
        # Calculate the values
        logging.debug("ReadMethod: %s", readMethod)
        logging.debug("WriteMethod: %s", writeMethod)
        try:
            readTotalMB = storageValues.get("Timing-%s-read-totalMegabytes" % readMethod, 0) \
                          + storageValues.get("Timing-%s-readv-totalMegabytes" % readMethod, 0)
            readMSecs = storageValues.get("Timing-%s-read-totalMsecs" % readMethod, 0) \
                        + storageValues.get("Timing-%s-readv-totalMsecs" % readMethod, 0)
            totalReads = storageValues.get("Timing-%s-read-numOperations" % readMethod, 0) \
                         + storageValues.get("Timing-%s-readv-numOperations" % readMethod, 0)
            readMaxMSec = max(storageValues.get("Timing-%s-read-maxMsecs" % readMethod, 0),
                              storageValues.get("Timing-%s-readv-maxMsecs" % readMethod, 0))
            readPercOps = storageValues.get("Timing-tstoragefile-readActual-numOperations", 0) / \
                          storageValues.get("Timing-tstoragefile-read-numOperations", 0)
            readCachOps = storageValues.get("Timing-tstoragefile-readViaCache-numSuccessfulOperations", 0) / \
                          storageValues.get("Timing-tstoragefile-read-numOperations", 0)
            readTotalT = storageValues.get("Timing-tstoragefile-read-totalMsecs", 0) / 1000
            readNOps = storageValues.get("Timing-tstoragefile-read-numOperations", 0)
            writeTime = storageValues.get("Timing-tstoragefile-write-totalMsecs", 0) / 1000
            writeTotMB = storageValues.get("Timing-%s-write-totalMegabytes" % writeMethod, 0) \
                         + storageValues.get("Timing-%s-writev-totalMegabytes" % writeMethod, 0)

            if readMSecs > 0:
                readMBSec = readTotalMB / readMSecs * 1000
            else:
                readMBSec = 0
            if totalReads > 0:
                readAveragekB = 1024 * readTotalMB / totalReads
            else:
                readAveragekB = 0

            # Attach them to the report
            setattr(report, 'readTotalMB', readTotalMB)
            setattr(report, 'readMBSec', readMBSec)
            setattr(report, 'readAveragekB', readAveragekB)
            setattr(report, 'readMaxMSec', readMaxMSec)
            setattr(report, 'readPercentageOps', readPercOps)
            setattr(report, 'readTotalSecs', readTotalT)
            setattr(report, 'readNumOps', readNOps)
            setattr(report, 'writeTotalSecs', writeTime)
            setattr(report, 'writeTotalMB', writeTotMB)
            setattr(report, 'readCachePercentageOps', readCachOps)
        except ZeroDivisionError:
            logging.error("Tried to divide by zero doing storage statistics report parsing.")
            logging.error("Either you aren't reading and writing data, or you aren't reporting it.")
            logging.error("Not adding any storage performance info to report.")


def xmlToJobReport(reportInstance, xmlFile):
    """
    _xmlToJobReport_

    parse the XML file and insert the information into the
    Report instance provided

    """
    # read XML, build node structure
    node = xmlFileToNode(xmlFile)

    #  //
    # // Set up coroutine pipeline
    # //
    fileDispatchers = {
        "Runs": runHandler(),
        "Branches": branchHandler(),
        "Inputs": inputAssocHandler(),
    }

    perfRepDispatchers = {
        "PerformanceSummary": perfSummaryHandler(),
        "CPU": perfCPUHandler(),
        "Memory": perfMemHandler(),
        "Storage": perfStoreHandler(),
    }

    dispatchers = {
        "File": fileHandler(fileDispatchers),
        "InputFile": inputFileHandler(fileDispatchers),
        "PerformanceReport": perfRepHandler(perfRepDispatchers),
        "AnalysisFile": analysisFileHandler(fileDispatchers),
        "FrameworkError": errorHandler(),
        "SkippedFile": skippedFileHandler(),
        "FallbackAttempt": fallbackAttemptHandler(),
        "SkippedEvent": skippedEventHandler(),
    }

    #  //
    # // Feed pipeline with node structure and report result instance
    # //
    reportBuilder(
        node, reportInstance,
        reportDispatcher(dispatchers)
    )

    return


childrenMatching = lambda node, nname: [x for x in node.children if x.name == nname]
