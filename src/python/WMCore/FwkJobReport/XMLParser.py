#!/usr/bin/env python
"""
_XMLParser_

Read the raw XML output from the cmsRun executable

"""

__version__ = "$Revision: 1.5 $"
__revision__ = "$Id: XMLParser.py,v 1.5 2010/03/12 20:24:03 sfoulkes Exp $"
__author__ = "evansde"


import xml.parsers.expat

from WMCore.Algorithms.ParseXMLFile import Node, xmlFileToNode, coroutine




def reportBuilder(nodeStruct, report, target):
    """
    _reportBuilder_

    Driver for coroutine pipe for building reports from the Node
    structure

    """
    for node in nodeStruct.children:
        target.send((report, node))


@coroutine
def reportDispatcher(targets):
    """
    _reportDispatcher_

    Top level routine for dispatching the parts of the job report to the
    handlers

    """
    while True:
        report, node = (yield)
        if node.name != "FrameworkJobReport":
            print "Not Handling: ", node.name
            #TODO: throw
            continue

        for subnode in node.children:
            if subnode.name == "File":
                targets['File'].send( (report, subnode) )
            elif subnode.name == "InputFile":
                targets['InputFile'].send( (report, subnode) )
            elif subnode.name == "AnalysisFile":
                targets['AnalysisFile'].send( (report, subnode) )
            elif subnode.name == "PerformanceReport":
                targets['PerformanceReport'].send( (report, subnode))
            elif subnode.name == "FrameworkError":
                targets['FrameworkError'].send( (report, subnode) )
            elif subnode.name == "SkippedFile":
                targets['SkippedFile'].send( (report, subnode) )
            elif subnode.name == "SkippedEvent":
                targets['SkippedEvent'].send( (report, subnode) )
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
        moduleNode = [ x for x in node.children if x.name == "ModuleLabel"][0]
        moduleName = moduleNode.text

        moduleRef = report.addOutputModule(moduleName)
        fileRef = report.addOutputFile(moduleName)
        fileAttrs = {}
        moduleRef.files.fileCount += 1
        for subnode in node.children:
            if subnode.name == "Inputs":
                targets['Inputs'].send( (fileRef, subnode) )
            elif subnode.name == "Runs":
                targets['Runs'].send( (fileRef, subnode) )
            elif subnode.name == "Branches":
                targets['Branches'].send( (fileRef, subnode) )
            else:
                fileAttrs[subnode.name] = subnode.text



        [ setattr(fileRef, k, v) for k, v in fileAttrs.items()]







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
        moduleNode = [ x for x in node.children if x.name == "ModuleLabel"][0]
        moduleName = moduleNode.text

        moduleRef = report.addInputSource(moduleName)
        fileRef = report.addInputFile(moduleName)
        fileAttrs = {}
        for subnode in node.children:
            if subnode.name == "Runs":
                targets['Runs'].send( (fileRef, subnode) )
            elif subnode.name == "Branches":
                targets['Branches'].send( (fileRef, subnode) )
            else:
                fileAttrs[subnode.name] = subnode.text

        [ setattr(fileRef, k, v) for k, v in fileAttrs.items()]

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

    Handle FrameworkError reports

    """
    while True:
        report, node = (yield)
        excepcode = node.attrs.get("ExitStatus", 8001)
        exceptype = node.attrs.get("Type", "CMSException")
        report.addError(excepcode, exceptype, node.text)

@coroutine
def skippedFileHandler():
    while True:
        report, node = (yield)
        lfn = node.attrs.get("Lfn", None)
        pfn = node.attrs.get("Pfn", None)
        report.addSkippedFile(lfn, pfn)


@coroutine
def skippedEventHandler():
    while True:
        report, node = (yield)
        run = node.attrs.get("Run", None)
        event = node.attrs.get("Event", None)
        if run == None: continue
        if event == None: continue
        report.addSkippedEvent(run, event)




@coroutine
def runHandler():
    """
    _runHandler_

    sink to pack run information into a file

    """
    while True:
        filedata, node = (yield)
        for subnode in node.children:
            if subnode.name == "Run":
                runId = subnode.attrs.get("ID", None)
                if runId == None: continue
                filedata.runs.section_(runId)
                runSect = getattr(filedata.runs, runId)
                lumis = [ lumi.attrs['ID']
                          for lumi in subnode.children
                          if lumi.attrs.has_key("ID")]
                runSect.lumiSections = lumis


@coroutine
def branchHandler():
    """
    _branchHandler_

    sink to pack branch information into a file

    """
    while True:
        filedata, node = (yield)
        branches = [ subnode.text for subnode in node.children
                     if subnode.name == "Branch" ]
        filedata.branches.names = branches

@coroutine
def inputAssocHandler():
    """
    _inputAssocHandler_

    sink to handle output:input association information

    """
    while True:
        filedata, node = (yield)
        fileCount = 0
        for inputnode in node.children:
            data = {}
            [ data.__setitem__(subnode.name, subnode.text)
              for subnode in inputnode.children]
            filelabel = "file%s" % fileCount
            #filedata.inputs.section_(filelabel)
            #entry = getattr(filedata.inputs, filelabel)
            #[ setattr(entry, k, v) for k,v in data.items() ]
            #fileCount +=1


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
        for subnode in node.children:
            if subnode.name == "PerformanceSummary":
                targets['PerformanceSummary'].send( (perfRep.summaries,
                                                     subnode) )
            if subnode.name == "CPU":
                targets['CPU'].send( (perfRep.cpu, subnode) )
            if subnode.name == "Memory":
                targets['Memory'].send( (perfRep.memory, subnode) )



@coroutine
def perfSummaryHandler():
    """
    _perfSummaryHandler_

    Sink to handle performance summaries

    """
    while True:
        report, node = (yield)
        summary = node.attrs.get('Metric', None)
        if summary == None: continue
        report.section_(summary)
        summRep = getattr(report, summary)
        [setattr(summRep, subnode.attrs['Name'], subnode.attrs['Value'])
         for subnode in node.children ]


@coroutine
def perfCPUHandler():
    """
    _perfCPUHandler_

    sink that packs CPU reports into the job report

    """
    while True:
        report, node = (yield)
        for subnode in node.children:
            if subnode.name == "CPUCore":
                corename = "Core%s" % subnode.attrs['Core']
                report.section_(corename)
                core = getattr(report, corename)
                [ setattr(core, prop.attrs['Name'], prop.text)
                  for prop in subnode.children]

@coroutine
def perfMemHandler():
    """
    _perfMemHandler_

    Pack memory performance reports into the report

    """
    while True:
        report, node = (yield)
        for prop in node.children:
            [setattr(report, prop.attrs['Name'], prop.text)
             for prop in node.children if prop.name == "Property"]




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
    #//
    fileDispatchers = {
        "Runs" : runHandler(),
        "Branches" : branchHandler(),
        "Inputs" : inputAssocHandler(),
        }

    perfRepDispatchers = {
        "PerformanceSummary" : perfSummaryHandler(),
        "CPU" : perfCPUHandler(),
        "Memory" : perfMemHandler(),
        }

    dispatchers  = {
        "File" : fileHandler(fileDispatchers),
        "InputFile": inputFileHandler(fileDispatchers),
        "PerformanceReport" : perfRepHandler(perfRepDispatchers),
        "AnalysisFile" : analysisFileHandler(fileDispatchers),
        "FrameworkError" : errorHandler(),
        "SkippedFile" : skippedFileHandler(),
        "SkippedEvent" : skippedEventHandler(),
        }

    #  //
    # // Feed pipeline with node structure and report result instance
    #//
    reportBuilder(
        node, reportInstance,
        reportDispatcher(dispatchers)
        )

    return

