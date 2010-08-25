#!/usr/bin/env python
"""
_ReportParser_

Read an XML Job Report into memory and create the appropriate FwkJobReport
instances and supporting objects.


"""

from xml.sax.handler import ContentHandler
from xml.sax import make_parser
from xml.sax import SAXParseException

from WMCore.FwkJobReport.FwkJobReport import FwkJobReport
from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery


class FwkJobRepHandler(ContentHandler):
    """
    _FwkJobRepHandler_

    SAX Content Handler implementation to build
    instances of FwkJobReport and populate it

    Multiple job reports in a file are supported, since the plan
    is to concatenate them at the end of a job from multiple outputs

    instances of FwkJobReport are stored in self.results as a list

    """
    def __init__(self):
        ContentHandler.__init__(self)
        #  //
        # // State containers used during parsing
        #//
        self.currentReport = FwkJobReport()
        self.currentFile = None
        self.currentDict = None

        self.currentInputDict = None
        self.currentCksum = None
        self.inTiming = False
        self.inGenInfo = False
        self._CharCache = ""

        #  //
        # // container for results
        #//
        self.results = []


        #  //
        # // Response methods to start of elements based on element name
        #//
        self._StartResponses = {
            "FrameworkJobReport" : self.newReport,
            "File" : self.newFile,
            "InputFile": self.newInputFile,
            "Dataset" : self.newDataset,
            "ExitCode" : self.exitCode,
            "State" : self.stateHandler,
            "Branch" : self.noResponse,
            "Branches" : self.noResponse,
            "Input" : self.startInput,
            "Inputs" : self.noResponse,
            "Runs" : self.noResponse,
            "Run" : self.noResponse,
            "SkippedEvent" : self.skippedEvent,
            "SkippedFile" : self.skippedFile,
            "Checksum" : self.checksum,
            "SiteDetail" : self.siteDetail,
            "FrameworkError" : self.frameworkError,
            "TimingService" : self.timingService,
            "StorageStatistics" : self.noResponse,
            "GeneratorInfo" : self.generatorInfo,
            "Data" : self.dataHandler,
            }

        #  //
        # // Response methods to end of elements based on element name
        #//
        self._EndResponses = {
            "FrameworkJobReport" : self.endReport,
            "File" : self.endFile,
            "Dataset" : self.endDataset,
            "ExitCode": self.noResponse,
            "State" : self.noResponse,
            "Branch" : self.endBranch,
            "Branches" : self.noResponse,
            "InputFile" : self.endInputFile,
            "Input" :self.endInput,
            "Inputs" : self.noResponse,
            "Runs" : self.noResponse,
            "Run" : self.endRun,
            "SkippedEvent" : self.noResponse,
            "SkippedFile" : self.noResponse,
            "Checksum" : self.endChecksum,
            "SiteDetail" : self.noResponse,
            "FrameworkError" : self.endFrameworkError,
            "TimingService" : self.endTimingService,
            "StorageStatistics" : self.storageStatistics,
            "GeneratorInfo" : self.endGeneratorInfo,
            "Data": self.noResponse,
            }

    def noResponse(self, name, attrs = {}):
        """some elements require no action"""
        if self.inTiming:
            value = str(attrs.get("Value"))
            self.currentDict[str(name)] = value
        pass


    def startElement(self, name, attrs):
        """
        _startElement_

        Override ContentHandler.startElement
        Start a new XML Element, call the appropriate response method based
        off the name of the element

        """
        response = self._StartResponses.get(name, self.noResponse)
        response(name, attrs)
        return

    def endElement(self, name):
        """
        _endElement_

        Override ContentHandler.endElement
        End of element, invoke response based on name and
        flush the chardata cache
        """
        response = self._EndResponses.get(name, self.fillDictionary)
        response(name)
        self._CharCache = ""
        return

    def characters(self, data):
        """
        _characters_

        Override ContentHandler.characters
        Accumulate character data from an xml element, if required
        the response will pick it up and insert it into the appropriate
        object.
        """
        if len(data.strip()) == 0:
            return
        self._CharCache += str(data).replace("\t", "")
        return


    def inFile(self):
        """boolean test to see if state is in a file block"""
        return self.currentFile != None


    def newReport(self, name, attrs):
        """
        _newReport_

        Handler method for a new FrameworkJobReport

        """
        self.currentReport = FwkJobReport()
        name =  attrs.get("Name", None)
        status = attrs.get("Status", None)
        jobSpec = attrs.get("JobSpecID", None)
        workSpec = attrs.get("WorkflowSpecID", None)
        jobType = attrs.get("JobType", None)
        if name != None:
            self.currentReport.name = str(name)
        if status != None:
            self.currentReport.status = str(status)
        if jobSpec != None:
            self.currentReport.jobSpecId = str(jobSpec)
        if jobType != None:
            self.currentReport.jobType = str(jobType)
        if workSpec != None:
            self.currentReport.workflowSpecId = str(workSpec)
        return

    def endReport(self, name):
        """
        _endReport_

        Handler Method for finishing a FrameorkJobReport
        """
        self.results.append(self.currentReport)
        self.currentReport = None
        return


    def newFile(self, name, attrs):
        """new File tag encountered"""
        self.currentFile = self.currentReport.newFile()
        self.currentDict = self.currentFile

    def endFile(self, name):
        """ end of file tag encountered"""
        self.currentFile = None
        self.currentDict = None

    def newInputFile(self, name, attrs):
        """new InputFile tag encountered"""
        self.currentFile = self.currentReport.newInputFile()
        self.currentDict = self.currentFile

    def endInputFile(self, name):
        """ end of InputFile tag encountered"""
        self.currentFile = None
        self.currentDict = None



    def newDataset(self, name, attrs):
        """ start of Dataset tag within a File tag"""
        if not self.inFile():
            return
        self.currentDict = self.currentFile.newDataset()
        return

    def endDataset(self, name):
        """end of Dataset tag"""
        if not self.inFile():
            return
        self.currentDict = self.currentFile


    def exitCode(self, name, attrs):
        """
        handle an ExitCode node, extract the value attr and add it to
        the current report

        """
        if self.currentReport == None:
            return

        value = attrs.get("Value", None)
        if value != None:
            self.currentReport.exitCode = int(value)
        return


    def fillDictionary(self, name):
        """
        _fillDictionary_

        Any object requiring population as a dictionary can use this
        handler to populate itself
        """
        if self.currentDict == None:
            return
        if self.inTiming:
            return
        self.currentDict[str(name)] = str(self._CharCache)
        return

    def stateHandler(self, name, attrs):
        """
        _stateHandler_

        Handle a State Node

        """
        self.currentFile.state = str(attrs.get('Value', "closed"))


    def endBranch(self, name):
        """
        _endBranch_

        End of Branch node

        """
        if self.currentFile != None:
            self.currentFile.branches.append(str(self._CharCache))

    def startInput(self, name, attrs):
        """
        _startInput_

        Handle Input reference for an Output File

        """
        if self.currentFile != None:
            if not self.currentFile.isInput:
                self.currentInputDict = {}
                self.currentDict = self.currentInputDict

    def endInput(self, name):
        """
        _endInput_

        end Input node
        """
        self.currentFile.addInputFile(self.currentInputDict['PFN'],
                                      self.currentInputDict['LFN'])

        self.currentInputDict = None
        self.currentDict = self.currentFile

    def endRun(self, name):
        """
        _endRun_

        Add Run Number chardata to current file
        """
        if self.currentFile != None:
            self.currentFile.runs.append(str(self._CharCache))


    def skippedEvent(self, name, attrs):
        """
        _skippedEvent_

        Record a Skipped Event

        """
        if self.currentReport == None:
            return
        run = attrs.get("Run", None)
        if run == None :
            return
        event = attrs.get("Event", None)
        if event == None:
            return
        self.currentReport.addSkippedEvent(str(run), str(event))
        return

    def skippedFile(self, name, attrs):
        """skipped file node"""
        if self.currentReport == None:
            return
        pfn = attrs.get("Pfn", None)
        lfn = attrs.get("Lfn", None)
        if pfn == None:
            return
        if lfn == None:
            return
        self.currentReport.addSkippedFile(str(pfn), str(lfn))
        return


    def checksum(self, name, attrs):
        """
        _checksum_

        Handle a checksum element start
        """
        self.currentCksum = attrs.get("Algorithm", None)


    def endChecksum(self, name):
        """
        _endChecksum_

        Handle a checksum element end
        """
        if self.currentCksum == None:
            return
        if not self.inFile():
            return
        self.currentFile.addChecksum(str(self.currentCksum),
                                     str(self._CharCache))
        self.currentCksum = None
        return

    def siteDetail(self, name, attrs):
        """
        _siteDetail_

        Handle a site detail parameter node

        """
        if self.currentReport == None:
            return
        detailName = attrs.get('Parameter', None)
        detailValue = attrs.get('Value' , None)
        if detailName == None:
            return
        self.currentReport.siteDetails[str(detailName)] = str(detailValue)
        return

    def frameworkError(self, name, attrs):
        """
        _frameworkError_

        Start Error message in job report

        """
        if self.currentReport == None:
            return
        errStatus = str(attrs.get("ExitStatus", "1"))
        errType  = str(attrs.get("Type", "Unknown"))
        self.currentDict = self.currentReport.addError(errStatus, errType)
        return

    def endFrameworkError(self, name):
        """
        _endFrameworkError_

        End Error block in job report

        """
        if self.currentReport == None:
            return
        if self.currentDict == None:
            return
        self.currentDict['Description'] = str(self._CharCache)
        self.currentDict == None
        return

    def timingService(self, name, attrs):
        """
        _timingService_

        Start of Timing Service element

        """
        if self.currentReport == None:
            return
        self.inTiming = True
        self.currentDict = self.currentReport.timing
        return

    def endTimingService(self, name):
        """
        _endTimingService_

        End of Timing Service element

        """

        if self.currentReport == None:
            return
        self.inTiming = False
        self.currentDict = None
        return

    def storageStatistics(self, name):
        """
        _storageStatistics_

        Big blob of chardata...

        """
        if self.currentReport == None:
            return
        self.currentReport.storageStatistics = str(self._CharCache)
        return


    def generatorInfo(self, name, attrs):
        """
        _generatorInfo_

        """
        if self.currentReport == None:
            return
        self.inGenInfo = True
        self.currentDict = self.currentReport.generatorInfo
        return

    def endGeneratorInfo(self, name):
        """
        _endGeneratorInfo_

        """
        self.inGenInfo = False
        self.currentDict = None
        return

    def dataHandler(self, name, attrs):
        """
        _dataHandler_

        """
        if self.inGenInfo:
            if self.currentDict == None:
                return
            key = str(attrs['Name'])
            val = str(attrs['Value'])
            self.currentDict[key] = val
            return
        return




def readJobReport(filename):
    """
    _readJobReport_

    Load an XML FwkJobReport Document into a FwkJobReport instance.
    return the FwkJobReport instances.

    Instantiate a new ContentHandler and run it over the file producing
    a list of FwkJobReport instances

    """
    #handler = FwkJobRepHandler()
    #parser = make_parser()
    #parser.setContentHandler(handler)
    #try:
    #    parser.parse(filename)
    #except SAXParseException, ex:
    #    msg = "Error parsing JobReport File: %s\n" % filename
    #    msg += str(ex)
    #    print msg
    #    return []
    #print handler.results[0]
    #return handler.results

    try:
        improvDoc = loadIMProvFile(filename)
    except Exception, ex:
        return []
    result = []
    reportQ = IMProvQuery("FrameworkJobReport")
    reportInstances = reportQ(improvDoc)
    for reportInstance in reportInstances:
        newReport = FwkJobReport()
        newReport.load(reportInstance)
        result.append(newReport)
    return result
