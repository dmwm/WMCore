#!/usr/bin/env python
"""
_XMLParser_

Read the raw XML output from the cmsRun executable. 
"""

__version__ = "$Revision: 1.7 $"
__revision__ = "$Id: XMLParser.py,v 1.7 2010/04/09 15:57:17 sfoulkes Exp $"

import xml.parsers.expat

from WMCore.FwkJobReport import Report
from WMCore.DataStructs.Run import Run
from WMCore.Algorithms.ParseXMLFile import Node, xmlFileToNode, coroutine

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
        for subnode in node.children:
            if subnode.name == "Inputs":
                targets['Inputs'].send( (fileRef, subnode) )
            elif subnode.name == "Runs":
                targets['Runs'].send( (fileRef, subnode) )
            elif subnode.name == "Branches":
                targets['Branches'].send( (fileRef, subnode) )
            else:
                fileAttrs[subnode.name] = subnode.text

        Report.addAttributesToFile(fileRef, lfn = fileAttrs["LFN"],
                                   pfn = fileAttrs["PFN"], catalog = fileAttrs["Catalog"],
                                   module_label = fileAttrs["ModuleLabel"],
                                   guid = fileAttrs["GUID"],
                                   ouput_module_class = fileAttrs["OutputModuleClass"],
                                   events = int(fileAttrs["TotalEvents"]),
                                   branch_hash = fileAttrs["BranchHash"])

        [fileRef]                

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
        moduleNode = [ x for x in node.children if x.name == "InputSourceClass"][0]
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

        Report.addAttributesToFile(fileRef, lfn = fileAttrs["LFN"],
                                   pfn = fileAttrs["PFN"], catalog = fileAttrs["Catalog"],
                                   module_label = fileAttrs["ModuleLabel"],
                                   guid = fileAttrs["GUID"], input_type = fileAttrs["InputType"],
                                   input_source_class = fileAttrs["InputSourceClass"],
                                   events = int(fileAttrs["EventsRead"]))

        [fileRef]

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

    Sink to add run information to a file.  Given the following XML:
      <Runs>
      <Run ID="122023">
        <LumiSection ID="215"/>
        <LumiSection ID="216"/>
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
                if runId == None: continue
                
                lumis = [ int(lumi.attrs['ID'])
                          for lumi in subnode.children
                          if lumi.attrs.has_key("ID")]

                runInfo = Run(runNumber = runId)
                runInfo.lumis.extend(lumis)

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
    """
    while True:
        fileSection, node = (yield)
        branches = [ subnode.text for subnode in node.children
                     if subnode.name == "Branch" ]
        Report.addBranchNamesToFile(fileSection, branches)

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
            [ data.__setitem__(subnode.name, subnode.text)
              for subnode in inputnode.children]
            Report.addInputToFile(fileSection, data["LFN"])

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

