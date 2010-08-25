#!/usr/bin/env python
"""
_XMLParser_

Read the raw XML output from the cmsRun executable

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: XMLParser.py,v 1.1 2009/11/11 00:35:53 evansde Exp $"
__author__ = "evansde"


import xml.parsers.expat


class Node:
    """
    _Node_

    Really simple DOM like container to simplify parsing the XML file
    and formatting the character data without all the whitespace guff

    """
    def __init__(self, name, attrs):
        self.name = str(name)
        self.attrs = {}
        self.text = None
        [ self.attrs.__setitem__(str(k), str(v)) for k,v in attrs.items()]
        self.children = []

    def __str__(self):

        result = " %s %s \"%s\"\n" % (self.name, self.attrs, self.text)
        for child in self.children:
            result += str(child)
        return result


def coroutine(func):
    """
    _coroutine_

    Decorator method used to prime coroutines

    """
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start


def expat_parse(f, target):
    """
    _expat_parse_

    Expat based XML parsing that feeds a node building coroutine

    """
    parser = xml.parsers.expat.ParserCreate()
    parser.buffer_size = 65536
    parser.buffer_text = True
    parser.returns_unicode = False
    parser.StartElementHandler = \
       lambda name,attrs: target.send(('start',(name,attrs)))
    parser.EndElementHandler = \
       lambda name: target.send(('end',name))
    parser.CharacterDataHandler = \
       lambda data: target.send(('text',data))
    parser.ParseFile(f)


@coroutine
def build(topNode):
    """
    _build_

    Node structure builder that is fed from the expat_parse method

    """
    nodeStack = [topNode]
    charCache = []
    while True:
        event, value = (yield)
        if event == "start":
            charCache = []
            newnode = Node(value[0], value[1])
            nodeStack[-1].children.append(newnode)
            nodeStack.append(newnode)

        elif event == "text":
            charCache.append(value)

        else: # end
            nodeStack[-1].text = str(''.join(charCache)).strip()
            nodeStack.pop()
            charCache = []

def xmlFileToNode(reportFile):
    """
    _xmlFileToNode_

    Use expat and the build coroutine to parse the XML file and build
    a node structure

    """
    node = Node("JobReports", {})
    expat_parse(open(reportFile, 'r'),
                build(node))
    return node




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
            filedata.inputs.section_(filelabel)
            entry = getattr(filedata.inputs, filelabel)
            [ setattr(entry, k, v) for k,v in data.items() ]
            fileCount +=1


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
        }

    #  //
    # // Feed pipeline with node structure and report result instance
    #//
    reportBuilder(
        node, reportInstance,
        reportDispatcher(dispatchers)
        )

    return

